import { record } from 'rrweb';

interface AnalyticsEvent {
  type:
    | 'page_view'
    | 'section_view'
    | 'visible_ms'
    | 'max_scroll'
    | 'cta_click'
    | 'form_seen'
    | 'form_submit'
    | 'rage_click'
    | 'dead_click'
    | 'page_end';
  at: number;
  data: Record<string, string | number | boolean | null>;
}

interface ReplayDecision {
  accepted?: boolean;
  replay?: {
    enabled: boolean;
    maxDurationMs: number;
    maxBytes: number;
  };
}

interface SectionState {
  visibleSince: number | null;
  totalMs: number;
  viewed: boolean;
}

const CTA_SELECTORS = [
  '[data-pixeo-cta]',
  '.pixeo-cta',
  '#trigger-buy',
  'a[href="mostrarBoton"]',
  'img[alt="CTA"]',
  '[data-pixeo-releasit-trigger]',
  '.pixeo-releasit-trigger',
  '[class*="releasit"] button',
  '[id*="releasit"] button',
  '[class*="easysell"] button',
  '[id*="easysell"] button',
  '[class*="easy-order"] button',
  'form[action*="order"] button[type="submit"]',
  'form[action*="order"] input[type="submit"]',
] as const;

function currentScript(): HTMLScriptElement | null {
  if (document.currentScript instanceof HTMLScriptElement) return document.currentScript;
  const scripts = Array.from(document.scripts);
  return scripts.find((script) => script.hasAttribute('data-pixeo-token')) ?? null;
}

function safePagePath(): string {
  return `${location.pathname}${location.hash}`.slice(0, 2048) || '/';
}

function referrerHost(): string {
  try {
    return document.referrer ? new URL(document.referrer).hostname.slice(0, 253) : '';
  } catch {
    return '';
  }
}

function deviceType(): 'mobile' | 'tablet' | 'desktop' | 'unknown' {
  const width = window.innerWidth;
  if (!Number.isFinite(width)) return 'unknown';
  if (width < 768) return 'mobile';
  if (width < 1024) return 'tablet';
  return 'desktop';
}

function utmContext(): Record<string, string> {
  const params = new URLSearchParams(location.search);
  const mapping = {
    source: 'utm_source',
    medium: 'utm_medium',
    campaign: 'utm_campaign',
    content: 'utm_content',
    term: 'utm_term',
  } as const;
  const result: Record<string, string> = {};
  for (const [key, parameter] of Object.entries(mapping)) {
    const value = params.get(parameter);
    if (value) result[key] = value.slice(0, 200);
  }
  return result;
}

function matchedCta(target: Element): string | null {
  for (const selector of CTA_SELECTORS) {
    if (target.closest(selector)) return selector;
  }
  return null;
}

function isInteractive(target: Element): boolean {
  return Boolean(
    target.closest(
      'a,button,input,select,textarea,label,summary,[role="button"],[role="link"],[contenteditable="true"]',
    ),
  );
}

function sectionIdOf(target: Element): string | null {
  const section = target.closest('[data-pixeo-item-id],[id^="pix-section-"]');
  if (!section) return null;
  const id = section.getAttribute('data-pixeo-item-id') ?? section.id.replace(/^pix-section-/, '');
  return id ? id.slice(0, 200) : null;
}

function describeTarget(target: Element): string {
  const tag = target.tagName.toLowerCase();
  if (target instanceof HTMLImageElement) {
    return `img${target.alt ? `: ${target.alt.trim().slice(0, 60)}` : ''}`;
  }
  const label = (target.textContent ?? '').replace(/\s+/g, ' ').trim().slice(0, 60);
  return label ? `${tag}: ${label}` : tag;
}

function clickContext(target: Element): Record<string, string | null> {
  return { sectionId: sectionIdOf(target), target: describeTarget(target) };
}

async function boot(): Promise<void> {
  const script = currentScript();
  const token = script?.dataset.pixeoToken;
  if (!script?.src || !token || !/^[A-Za-z0-9_-]{43}$/.test(token)) return;

  const scriptUrl = new URL(script.src, location.href);
  const endpoint = `${scriptUrl.origin}/api/v1/insights/collect/${token}`;
  const sessionId = crypto.randomUUID();
  let sequence = 0;
  let queue = Promise.resolve();
  let analyticsBuffer: AnalyticsEvent[] = [];
  let replayBuffer: unknown[] = [];
  let replayBufferBytes = 0;
  let replayTotalBytes = 0;
  let stopReplay: (() => void) | undefined;
  let stopped = false;

  const post = async (payload: Readonly<Record<string, unknown>>): Promise<ReplayDecision | null> => {
    try {
      const body = JSON.stringify(payload);
      const response = await fetch(endpoint, {
        method: 'POST',
        mode: 'cors',
        credentials: 'omit',
        // keepalive falla con cuerpos > 64KB (p. ej. el FullSnapshot).
        keepalive: body.length < 60_000,
        headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
        body,
      });
      if (!response.ok) return null;
      const value: unknown = await response.json();
      if (!value || typeof value !== 'object') return null;
      return value;
    } catch {
      return null;
    }
  };

  const enqueue = (payload: Readonly<Record<string, unknown>>): void => {
    queue = queue.then(async () => {
      const result = await post(payload);
      if (payload.kind === 'replay' && result?.accepted === false) {
        stopReplay?.();
        stopReplay = undefined;
      }
    });
  };

  const commonBatch = (): Record<string, unknown> => ({
    version: 1,
    sessionId,
    sequence: sequence++,
    sentAt: Date.now(),
    pagePath: safePagePath(),
  });

  const flushAnalytics = (): void => {
    if (analyticsBuffer.length === 0) return;
    const events = analyticsBuffer;
    analyticsBuffer = [];
    enqueue({ ...commonBatch(), kind: 'analytics', events });
  };

  const emit = (type: AnalyticsEvent['type'], data: AnalyticsEvent['data'] = {}): void => {
    analyticsBuffer.push({ type, at: Date.now(), data });
    if (analyticsBuffer.length >= 20) flushAnalytics();
  };

  const flushReplay = (): void => {
    if (replayBuffer.length === 0) return;
    const events = replayBuffer;
    replayBuffer = [];
    replayBufferBytes = 0;
    enqueue({ ...commonBatch(), kind: 'replay', events });
  };

  const startResponse = await post({
    ...commonBatch(),
    kind: 'session_start',
    context: {
      referrerHost: referrerHost(),
      device: deviceType(),
      viewportWidth: Math.max(0, Math.round(window.innerWidth)),
      viewportHeight: Math.max(0, Math.round(window.innerHeight)),
      language: navigator.language.slice(0, 35),
      utm: utmContext(),
    },
  });
  if (!startResponse?.accepted) return;

  emit('page_view');

  const sections = new Map<Element, SectionState>();
  const sectionObserver = new IntersectionObserver(
    (entries) => {
      const now = performance.now();
      for (const entry of entries) {
        const state = sections.get(entry.target);
        if (!state) continue;
        if (
          entry.isIntersecting
          && (entry.intersectionRatio >= 0.25 || entry.intersectionRect.height >= window.innerHeight * 0.4)
        ) {
          if (!state.viewed) {
            state.viewed = true;
            const sectionId = entry.target.getAttribute('data-pixeo-item-id')
              ?? entry.target.id.replace(/^pix-section-/, '');
            emit('section_view', { sectionId: sectionId.slice(0, 200) });
          }
          state.visibleSince ??= now;
        } else if (state.visibleSince !== null) {
          state.totalMs += now - state.visibleSince;
          state.visibleSince = null;
        }
      }
    },
    { threshold: [0, 0.1, 0.25, 0.5] },
  );
  // Fallback: si la landing publicada no trae ids, numerar los bloques del contenido.
  let blockIndex = 0;
  for (const block of document.querySelectorAll(
    '.product-landing-content .image-container, .product-landing-content .section-container',
  )) {
    blockIndex += 1;
    if (!block.getAttribute('data-pixeo-item-id') && !block.id.startsWith('pix-section-')) {
      block.setAttribute('data-pixeo-item-id', `bloque-${blockIndex}`);
    }
  }
  for (const section of document.querySelectorAll('[id^="pix-section-"],[data-pixeo-item-id]')) {
    sections.set(section, { visibleSince: null, totalMs: 0, viewed: false });
    sectionObserver.observe(section);
  }

  const seenForms = new WeakSet<Element>();
  const formObserver = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (entry.isIntersecting && !seenForms.has(entry.target)) {
          seenForms.add(entry.target);
          emit('form_seen');
          formObserver.unobserve(entry.target);
        }
      }
    },
    { threshold: [0.1] },
  );
  const formSurfaceSelector = [
    'form',
    '#rsi_form_wrapper',
    '#_rsi-form-container-embedded',
    '[class*="es-popup"]',
    '[class*="es-cod"]',
    '[class*="es-order-form"]',
    '[class*="easysell"]',
  ].join(',');
  const observeFormSurfaces = (root: ParentNode): void => {
    const surfaces = root instanceof Element && root.matches(formSurfaceSelector)
      ? [root, ...root.querySelectorAll(formSurfaceSelector)]
      : [...root.querySelectorAll(formSurfaceSelector)];
    for (const surface of surfaces) {
      if (!seenForms.has(surface)) formObserver.observe(surface);
    }
  };
  observeFormSurfaces(document);

  document.addEventListener(
    'submit',
    () => {
      emit('form_submit');
      flushAnalytics();
    },
    { capture: true, passive: true },
  );

  let mutationVersion = 0;
  const mutationObserver = new MutationObserver((records) => {
    mutationVersion += 1;
    for (const record of records) {
      for (const node of record.addedNodes) {
        if (node instanceof Element) observeFormSurfaces(node);
      }
    }
  });
  mutationObserver.observe(document.documentElement, {
    childList: true,
    subtree: true,
    attributes: true,
  });
  const recentClicks: { at: number; x: number; y: number }[] = [];
  document.addEventListener(
    'click',
    (event) => {
      if (!(event.target instanceof Element)) return;
      const target = event.target;
      const cta = matchedCta(target);
      if (cta) emit('cta_click', { selector: cta });

      const now = Date.now();
      recentClicks.push({ at: now, x: event.clientX, y: event.clientY });
      while (recentClicks[0] && now - recentClicks[0].at > 1000) recentClicks.shift();
      const nearby = recentClicks.filter(
        (click) => Math.hypot(click.x - event.clientX, click.y - event.clientY) <= 30,
      );
      if (nearby.length === 3) emit('rage_click', clickContext(target));

      if (!cta && !isInteractive(target)) {
        const versionAtClick = mutationVersion;
        window.setTimeout(() => {
          if (mutationVersion === versionAtClick && document.contains(target)) {
            emit('dead_click', clickContext(target));
          }
        }, 800);
      }
    },
    { capture: true, passive: true },
  );

  let maxScroll = 0;
  let scrollScheduled = false;
  const updateScroll = (): void => {
    const available = Math.max(1, document.documentElement.scrollHeight - window.innerHeight);
    maxScroll = Math.max(maxScroll, Math.min(100, (window.scrollY / available) * 100));
    scrollScheduled = false;
  };
  window.addEventListener(
    'scroll',
    () => {
      if (!scrollScheduled) {
        scrollScheduled = true;
        requestAnimationFrame(updateScroll);
      }
    },
    { passive: true },
  );

  if (startResponse.replay?.enabled) {
    const maxBytes = Math.min(3_145_728, startResponse.replay.maxBytes);
    const maxDurationMs = Math.min(180_000, startResponse.replay.maxDurationMs);
    stopReplay = record({
      emit(event) {
        const eventBytes = new TextEncoder().encode(JSON.stringify(event)).byteLength;
        // Meta (4) y FullSnapshot (2) se guardan siempre: sin ellos el replay queda en blanco.
        const eventType: number = event.type;
        const isSnapshot = eventType === 2 || eventType === 4;
        if (!isSnapshot && replayTotalBytes + eventBytes > maxBytes) {
          flushReplay();
          stopReplay?.();
          stopReplay = undefined;
          return;
        }
        replayBuffer.push(event);
        replayBufferBytes += eventBytes;
        replayTotalBytes += eventBytes;
        if (eventType === 2 || replayBufferBytes >= 50_000 || replayBuffer.length >= 400) {
          flushReplay();
        }
      },
      inlineStylesheet: true,
      maskAllInputs: true,
      blockSelector:
        '.pixeo-insights-block,iframe,[data-sensitive],[data-private],input[type="password"],input[autocomplete^="cc-"]',
      recordCrossOriginIframes: false,
      collectFonts: false,
      inlineImages: false,
    });
    window.setTimeout(() => {
      flushReplay();
      stopReplay?.();
      stopReplay = undefined;
    }, maxDurationMs);
    window.setInterval(flushReplay, 5000);
  }

  window.setInterval(flushAnalytics, 10_000);
  const endPage = (): void => {
    if (stopped) return;
    stopped = true;
    const now = performance.now();
    for (const [section, state] of sections.entries()) {
      if (state.visibleSince !== null) state.totalMs += now - state.visibleSince;
      if (state.totalMs > 0) {
        const sectionId = section.getAttribute('data-pixeo-item-id')
          ?? section.id.replace(/^pix-section-/, '');
        emit('visible_ms', {
          sectionId: sectionId.slice(0, 200),
          milliseconds: Math.round(state.totalMs),
        });
      }
    }
    emit('max_scroll', { percent: Math.round(maxScroll * 100) / 100 });
    emit('page_end');
    flushReplay();
    stopReplay?.();
    flushAnalytics();
  };
  window.addEventListener('pagehide', endPage, { once: true, passive: true });
  document.addEventListener(
    'visibilitychange',
    () => {
      if (document.visibilityState === 'hidden') flushAnalytics();
    },
    { passive: true },
  );
}

void boot().catch(() => undefined);
