import { randomBytes, randomUUID } from 'node:crypto';

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import type { QueryResultRow } from 'pg';
import { z } from 'zod';

import type { SupabaseAuth } from './auth.js';
import type { CollectorService, DeploymentRow } from './collector.js';
import type { AppConfig } from './config.js';
import type { Database } from './database.js';
import { FixedWindowRateLimiter } from './rateLimiter.js';
import type { ReplayStore } from './replayStore.js';
import {
  collectorBatchSchema,
  confirmDeploymentBodySchema,
  deploymentBodySchema,
  deploymentParamsSchema,
  landingParamsSchema,
  landingSessionParamsSchema,
  rangeSchema,
  sessionsQuerySchema,
  tokenParamsSchema,
} from './schemas.js';
import { isCompatibleOrigin, normalizeShopDomain, sha256 } from './security.js';

const API_PREFIX = '/api/v1/insights';

interface RouteDependencies {
  auth: SupabaseAuth;
  collector: CollectorService;
  config: AppConfig;
  database: Database;
  replayStore: ReplayStore;
  trackerBundle: Buffer;
}

interface BootstrapRow extends QueryResultRow {
  id: string;
  public_token: string;
}

interface PublicationRow extends QueryResultRow {
  id: string;
  shop_domain: string;
  product_id: string;
  product_handle: string;
  published_url: string;
  published_at: Date;
}

interface SummaryRow extends QueryResultRow {
  sessions: string;
  page_views: string;
  cta_clicks: string;
  form_submits: string;
  rage_clicks: string;
  dead_clicks: string;
  average_duration_ms: string | null;
  average_max_scroll: string | null;
}

interface SectionSummaryRow extends QueryResultRow {
  section_id: string;
  reached_sessions: string;
  average_visible_ms: string | null;
  first_seen_at: Date;
}

interface SessionRow extends QueryResultRow {
  deployment_id: string;
  session_id: string;
  landing_id: string;
  started_at: Date;
  last_seen_at: Date;
  ended_at: Date | null;
  page_path: string;
  context: unknown;
  duration_ms: string | null;
  device: string | null;
  max_scroll_depth: string | null;
  cta_clicks: string;
  form_submissions: string;
  replay_event_count: number;
  replay_available: boolean;
}

interface AnalyticsRow extends QueryResultRow {
  type: string;
  at: Date;
  data: unknown;
}

interface ReplayFileRow extends QueryResultRow {
  file_name: string;
}

const cursorSchema = z
  .object({
    startedAt: z.iso.datetime(),
    deploymentId: z.uuid(),
    sessionId: z.uuid(),
  })
  .strict();

interface FunnelRow extends QueryResultRow {
  section_id: string;
  reached: string;
  dropoff: string;
  avg_visible_ms: string | null;
  dead_clicks: string;
  rage_clicks: string;
}

interface DeadTargetRow extends QueryResultRow {
  section_id: string;
  target: string;
  count: string;
}

interface VisitsRow extends QueryResultRow {
  visits: string;
  cta_sessions: string;
}

function replayMeta(event: unknown): { type: number; timestamp: number } {
  if (event && typeof event === 'object') {
    const { type, timestamp } = event as { type?: unknown; timestamp?: unknown };
    return {
      type: typeof type === 'number' ? type : -1,
      timestamp: typeof timestamp === 'number' ? timestamp : 0,
    };
  }
  return { type: -1, timestamp: 0 };
}

/** Ordena por timestamp (Meta antes que FullSnapshot en empate) y deja el primer snapshot al inicio. */
function sortReplayEvents(events: unknown[]): void {
  const rank = (type: number): number => (type === 4 ? 0 : type === 2 ? 1 : 2);
  events.sort((a, b) => {
    const left = replayMeta(a);
    const right = replayMeta(b);
    return left.timestamp - right.timestamp || rank(left.type) - rank(right.type);
  });
  const fullIndex = events.findIndex((event) => replayMeta(event).type === 2);
  if (fullIndex <= 0) return;
  let metaIndex = -1;
  for (let index = fullIndex - 1; index >= 0; index -= 1) {
    if (replayMeta(events[index]).type === 4) {
      metaIndex = index;
      break;
    }
  }
  const full = events.splice(fullIndex, 1);
  const meta = metaIndex >= 0 ? events.splice(metaIndex, 1) : [];
  events.unshift(...meta, ...full);
}

function ownerId(request: FastifyRequest): string {
  if (!request.auth) throw new Error('Contexto de autenticación ausente');
  return request.auth.ownerId;
}

function requestOrigin(request: FastifyRequest): string | undefined {
  const origin = request.headers.origin;
  if (origin) return origin;
  const referer = request.headers.referer;
  if (!referer) return undefined;
  try {
    return new URL(referer).origin;
  } catch {
    return undefined;
  }
}

function parseBody(body: unknown): unknown {
  if (typeof body !== 'string') return body;
  return JSON.parse(body);
}

function parseCursor(value: string | undefined): z.infer<typeof cursorSchema> | null {
  if (!value) return null;
  try {
    const decoded: unknown = JSON.parse(Buffer.from(value, 'base64url').toString('utf8'));
    return cursorSchema.parse(decoded);
  } catch {
    throw new Error('Cursor inválido');
  }
}

function encodeCursor(row: SessionRow): string {
  return Buffer.from(
    JSON.stringify({
      startedAt: row.started_at.toISOString(),
      deploymentId: row.deployment_id,
      sessionId: row.session_id,
    }),
  ).toString('base64url');
}

function setCors(reply: FastifyReply, origin: string): void {
  void reply.header('Access-Control-Allow-Origin', origin);
  void reply.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  void reply.header('Access-Control-Allow-Headers', 'Authorization,Content-Type,apikey');
  void reply.header('Access-Control-Max-Age', '600');
  void reply.header('Vary', 'Origin');
}

async function deploymentForCollector(
  request: FastifyRequest,
  reply: FastifyReply,
  collector: CollectorService,
): Promise<DeploymentRow | null> {
  const params = tokenParamsSchema.safeParse(request.params);
  if (!params.success) {
    await reply.code(404).send({ error: 'not_found' });
    return null;
  }
  const deployment = await collector.findDeployment(sha256(params.data.token));
  if (!deployment) {
    await reply.code(404).send({ error: 'not_found' });
    return null;
  }
  const origin = requestOrigin(request);
  if (!origin || !isCompatibleOrigin(origin, deployment.shop_domain)) {
    await reply.code(403).send({ error: 'origin_not_allowed' });
    return null;
  }
  setCors(reply, origin);
  return deployment;
}

export function registerRoutes(app: FastifyInstance, dependencies: RouteDependencies): void {
  const { auth, collector, config, database, replayStore, trackerBundle } = dependencies;
  const limiter = new FixedWindowRateLimiter(config.collectRateLimitPerMinute);
  const trackerEtag = `"${sha256(trackerBundle.toString('binary'))}"`;

  app.get('/health', () => ({ status: 'ok' }));
  app.get('/ready', async (_request, reply) => {
    const ready = await database.ready();
    return ready
      ? reply.send({ status: 'ready' })
      : reply.code(503).send({ status: 'not_ready' });
  });

  app.get('/bootstrap.js', async (request, reply) => {
    const query = z.object({
      shop: z.string().trim().min(1).max(253),
      product_id: z.coerce.string().trim().min(1).max(255),
    }).strict().safeParse(request.query);
    if (!query.success) {
      return reply.code(400).type('application/javascript').send('');
    }

    let shopDomain: string;
    try {
      shopDomain = normalizeShopDomain(query.data.shop);
    } catch {
      return reply.code(400).type('application/javascript').send('');
    }

    const result = await database.query<BootstrapRow>(
      `SELECT id, public_token
       FROM insights_deployments
       WHERE shop_domain = $1 AND product_id = $2 AND active = true
         AND public_token IS NOT NULL
       ORDER BY created_at DESC
       LIMIT 1`,
      [shopDomain, query.data.product_id],
    );
    const deployment = result.rows[0];
    if (!deployment) {
      return reply.type('application/javascript').header('Cache-Control', 'no-store').send('');
    }

    const script = `(function(){var s=document.createElement('script');s.src=${JSON.stringify(`${config.publicBaseUrl}/tracker.js`)};s.defer=true;s.dataset.pixeoToken=${JSON.stringify(deployment.public_token)};s.dataset.pixeoDeployment=${JSON.stringify(deployment.id)};document.head.appendChild(s);}());`;
    return reply
      .type('application/javascript; charset=utf-8')
      .header('Cache-Control', 'no-store')
      .send(script);
  });

  app.get('/tracker.js', async (request, reply) => {
    if (request.headers['if-none-match'] === trackerEtag) return reply.code(304).send();
    return reply
      .type('application/javascript; charset=utf-8')
      .header('Cache-Control', 'public, max-age=3600, stale-while-revalidate=86400')
      .header('ETag', trackerEtag)
      .send(trackerBundle);
  });

  app.options(`${API_PREFIX}/collect/:token`, async (request, reply) => {
    const deployment = await deploymentForCollector(request, reply, collector);
    if (!deployment) return;
    await reply.code(204).send();
  });

  app.post(
    `${API_PREFIX}/collect/:token`,
    { bodyLimit: config.collectBodyLimitBytes },
    async (request, reply) => {
      const deployment = await deploymentForCollector(request, reply, collector);
      if (!deployment) return;
      const rateKey = `${deployment.id}:${sha256(request.ip).slice(0, 16)}`;
      if (!limiter.consume(rateKey)) {
        await reply.code(429).send({ error: 'rate_limited' });
        return;
      }

      let rawBody: unknown;
      try {
        rawBody = parseBody(request.body);
      } catch {
        await reply.code(400).send({ error: 'invalid_payload' });
        return;
      }
      const body = collectorBatchSchema.safeParse(rawBody);
      if (!body.success) {
        await reply.code(400).send({ error: 'invalid_payload' });
        return;
      }

      if (body.data.kind === 'session_start') {
        const result = await collector.startSession(deployment, body.data);
        await reply.code(202).send(result);
        return;
      }
      if (body.data.kind === 'analytics') {
        const result = await collector.ingestAnalytics(deployment, body.data);
        await reply.code(202).send(result);
        return;
      }

      const result = await collector.ingestReplay(deployment, body.data);
      await reply.code(202).send(result);
    },
  );

  app.options(`${API_PREFIX}/*`, async (request, reply) => {
    const origin = request.headers.origin;
    if (!origin || !config.dashboardOrigins.has(origin)) {
      await reply.code(403).send({ error: 'origin_not_allowed' });
      return;
    }
    setCors(reply, origin);
    await reply.code(204).send();
  });

  app.addHook('onRequest', async (request, reply) => {
    const origin = request.headers.origin;
    if (origin && config.dashboardOrigins.has(origin)) setCors(reply, origin);
  });

  app.post(
    `${API_PREFIX}/deployments`,
    { preHandler: auth.authenticate.bind(auth) },
    async (request, reply) => {
      const body = deploymentBodySchema.safeParse(request.body);
      if (!body.success) {
        await reply.code(400).send({ error: 'invalid_payload' });
        return;
      }

      let shopDomain: string;
      try {
        shopDomain = normalizeShopDomain(body.data.shopDomain);
      } catch {
        await reply.code(400).send({ error: 'invalid_shop_domain' });
        return;
      }

      const deploymentId = randomUUID();
      const token = randomBytes(32).toString('base64url');
      const tokenHash = sha256(token);
      await database.transaction(async (client) => {
        await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [
          `${ownerId(request)}:${body.data.landingId}:${body.data.storeId ?? ''}:${shopDomain}:${body.data.productId}`,
        ]);
        await client.query(
          `INSERT INTO insights_deployments (
             id, owner_id, landing_id, landing_name, store_id,
             shop_domain, product_id, token_hash, public_token
           ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
          [
            deploymentId,
            ownerId(request),
            body.data.landingId,
            body.data.landingName,
            body.data.storeId ?? null,
            shopDomain,
            body.data.productId,
            tokenHash,
            token,
          ],
        );
      });

      await reply.code(201).send({
        deploymentId,
        token,
        trackerUrl: `${config.publicBaseUrl}/tracker.js`,
      });
    },
  );

  app.post(
    `${API_PREFIX}/deployments/:deploymentId/confirm`,
    { preHandler: auth.authenticate.bind(auth) },
    async (request, reply) => {
      const params = deploymentParamsSchema.safeParse(request.params);
      const body = confirmDeploymentBodySchema.safeParse(request.body);
      if (!params.success || !body.success) {
        await reply.code(400).send({ error: 'invalid_request' });
        return;
      }

      const result = await database.query<PublicationRow>(
        `UPDATE insights_deployments
         SET published = true, product_handle = $3, published_url = $4, published_at = now()
         WHERE id = $1 AND owner_id = $2
         RETURNING id, shop_domain, product_id, product_handle, published_url, published_at`,
        [params.data.deploymentId, ownerId(request), body.data.productHandle, body.data.publishedUrl],
      );
      const publication = result.rows[0];
      if (!publication) {
        await reply.code(404).send({ error: 'not_found' });
        return;
      }
      await reply.send({
        deploymentId: publication.id,
        shopDomain: publication.shop_domain,
        productId: publication.product_id,
        productHandle: publication.product_handle,
        publishedUrl: publication.published_url,
        publishedAt: publication.published_at,
      });
    },
  );

  app.get(
    `${API_PREFIX}/landings/:landingId/publication`,
    { preHandler: auth.authenticate.bind(auth) },
    async (request, reply) => {
      const params = landingParamsSchema.safeParse(request.params);
      if (!params.success) {
        await reply.code(400).send({ error: 'invalid_request' });
        return;
      }
      const result = await database.query<PublicationRow>(
        `SELECT id, shop_domain, product_id, product_handle, published_url, published_at
         FROM insights_deployments
         WHERE owner_id = $1 AND landing_id = $2 AND published = true
         ORDER BY published_at DESC
         LIMIT 1`,
        [ownerId(request), params.data.landingId],
      );
      const publication = result.rows[0];
      await reply.send(publication ? {
        published: true,
        deploymentId: publication.id,
        shopDomain: publication.shop_domain,
        productId: publication.product_id,
        productHandle: publication.product_handle,
        publishedUrl: publication.published_url,
        publishedAt: publication.published_at,
      } : { published: false });
    },
  );

  app.get(
    `${API_PREFIX}/landings/:landingId/summary`,
    { preHandler: auth.authenticate.bind(auth) },
    async (request, reply) => {
      const params = landingParamsSchema.safeParse(request.params);
      const queryObject = z.object({ range: rangeSchema.default('7d') }).strict().safeParse(request.query);
      if (!params.success || !queryObject.success) {
        await reply.code(400).send({ error: 'invalid_request' });
        return;
      }
      const interval = { '24h': '24 hours', '7d': '7 days', '30d': '30 days' }[
        queryObject.data.range
      ];
      const funnelArgs = [ownerId(request), params.data.landingId, interval];
      const [visitsResult, funnelResult, deadTargetsResult] = await Promise.all([
        database.query<VisitsRow>(
          `SELECT
             COUNT(DISTINCT (deployment_id, session_id))
               FILTER (WHERE event_type = 'page_view')::text AS visits,
             COUNT(DISTINCT (deployment_id, session_id))
               FILTER (WHERE event_type = 'cta_click')::text AS cta_sessions
           FROM insights_analytics_events
           WHERE owner_id = $1 AND landing_id = $2
             AND occurred_at >= now() - $3::interval
             AND event_type IN ('page_view', 'cta_click')`,
          funnelArgs,
        ),
        database.query<FunnelRow>(
          `WITH ev AS (
             SELECT deployment_id, session_id, event_type, occurred_at,
                    payload, payload->>'sectionId' AS sid
             FROM insights_analytics_events
             WHERE owner_id = $1 AND landing_id = $2
               AND occurred_at >= now() - $3::interval
               AND event_type IN ('section_view', 'visible_ms', 'dead_click', 'rage_click')
               AND COALESCE(payload->>'sectionId', '') <> ''
           ), views AS (
             SELECT deployment_id, session_id, sid, MIN(occurred_at) AS at
             FROM ev WHERE event_type = 'section_view'
             GROUP BY deployment_id, session_id, sid
           ), starts AS (
             SELECT deployment_id, session_id, MIN(at) AS start_at
             FROM views GROUP BY deployment_id, session_id
           ), reach AS (
             SELECT v.sid, COUNT(*) AS reached,
                    AVG(EXTRACT(EPOCH FROM (v.at - s.start_at))) AS avg_offset,
                    MIN(v.at) AS first_at
             FROM views v JOIN starts s USING (deployment_id, session_id)
             GROUP BY v.sid
           ), last_views AS (
             SELECT DISTINCT ON (deployment_id, session_id) sid
             FROM views ORDER BY deployment_id, session_id, at DESC
           ), drops AS (
             SELECT sid, COUNT(*) AS dropoff FROM last_views GROUP BY sid
           ), stats AS (
             SELECT sid,
                    AVG(CASE WHEN event_type = 'visible_ms'
                             AND payload->>'milliseconds' ~ '^[0-9]+$'
                             THEN (payload->>'milliseconds')::numeric END) AS avg_visible_ms,
                    COUNT(*) FILTER (WHERE event_type = 'dead_click') AS dead_clicks,
                    COUNT(*) FILTER (WHERE event_type = 'rage_click') AS rage_clicks
             FROM ev GROUP BY sid
           )
           SELECT r.sid AS section_id, r.reached::text,
                  COALESCE(d.dropoff, 0)::text AS dropoff,
                  st.avg_visible_ms::text,
                  COALESCE(st.dead_clicks, 0)::text AS dead_clicks,
                  COALESCE(st.rage_clicks, 0)::text AS rage_clicks
           FROM reach r
           LEFT JOIN drops d ON d.sid = r.sid
           LEFT JOIN stats st ON st.sid = r.sid
           ORDER BY r.avg_offset, r.first_at`,
          funnelArgs,
        ),
        database.query<DeadTargetRow>(
          `SELECT payload->>'sectionId' AS section_id, payload->>'target' AS target,
                  COUNT(*)::text AS count
           FROM insights_analytics_events
           WHERE owner_id = $1 AND landing_id = $2
             AND occurred_at >= now() - $3::interval
             AND event_type = 'dead_click'
             AND COALESCE(payload->>'sectionId', '') <> ''
             AND COALESCE(payload->>'target', '') <> ''
           GROUP BY 1, 2
           ORDER BY COUNT(*) DESC`,
          funnelArgs,
        ),
      ]);
      const visits = Number(visitsResult.rows[0]?.visits ?? 0);
      const ctaSessions = Number(visitsResult.rows[0]?.cta_sessions ?? 0);
      const pct = (value: number): number => (visits > 0 ? (value / visits) * 100 : 0);
      const deadTargets = new Map<string, { target: string; count: number }[]>();
      for (const deadRow of deadTargetsResult.rows) {
        const list = deadTargets.get(deadRow.section_id) ?? [];
        if (list.length < 5) list.push({ target: deadRow.target, count: Number(deadRow.count) });
        deadTargets.set(deadRow.section_id, list);
      }
      const funnel = funnelResult.rows.map((funnelRow, index) => {
        const reached = Number(funnelRow.reached);
        return {
          sectionId: funnelRow.section_id,
          order: index + 1,
          reached,
          reachedPct: pct(reached),
          dropoffPct: pct(Number(funnelRow.dropoff)),
          avgVisibleMs: funnelRow.avg_visible_ms === null ? null : Number(funnelRow.avg_visible_ms),
          deadClicks: Number(funnelRow.dead_clicks),
          rageClicks: Number(funnelRow.rage_clicks),
          topDeadTargets: deadTargets.get(funnelRow.section_id) ?? [],
        };
      });

      const [summaryResult, sectionsResult] = await Promise.all([
        database.query<SummaryRow>(
          `WITH selected_sessions AS (
             SELECT deployment_id, session_id, started_at, last_seen_at
             FROM insights_sessions
             WHERE owner_id = $1 AND landing_id = $2
               AND started_at >= now() - $3::interval
           ), selected_events AS (
             SELECT event_type, payload, deployment_id, session_id
             FROM insights_analytics_events
             WHERE owner_id = $1 AND landing_id = $2
               AND occurred_at >= now() - $3::interval
           )
           SELECT
             (SELECT COUNT(*) FROM selected_sessions)::text AS sessions,
             COUNT(*) FILTER (WHERE event_type = 'page_view')::text AS page_views,
             COUNT(*) FILTER (WHERE event_type = 'cta_click')::text AS cta_clicks,
             COUNT(*) FILTER (WHERE event_type = 'form_submit')::text AS form_submits,
             COUNT(*) FILTER (WHERE event_type = 'rage_click')::text AS rage_clicks,
             COUNT(*) FILTER (WHERE event_type = 'dead_click')::text AS dead_clicks,
             (SELECT AVG(EXTRACT(EPOCH FROM (last_seen_at - started_at)) * 1000)
                FROM selected_sessions)::text AS average_duration_ms,
             AVG(CASE WHEN event_type = 'max_scroll'
                      AND payload->>'percent' ~ '^[0-9]+(\\.[0-9]+)?$'
                      THEN (payload->>'percent')::numeric END)::text AS average_max_scroll
           FROM selected_events`,
          [ownerId(request), params.data.landingId, interval],
        ),
        database.query<SectionSummaryRow>(
          `WITH section_events AS (
             SELECT deployment_id, session_id, event_type, occurred_at, payload
             FROM insights_analytics_events
             WHERE owner_id = $1 AND landing_id = $2
               AND occurred_at >= now() - $3::interval
               AND event_type IN ('section_view', 'visible_ms')
               AND COALESCE(payload->>'sectionId', '') <> ''
           )
           SELECT
             payload->>'sectionId' AS section_id,
             COUNT(DISTINCT (deployment_id, session_id))
               FILTER (WHERE event_type = 'section_view')::text AS reached_sessions,
             AVG(CASE WHEN event_type = 'visible_ms'
                      AND payload->>'milliseconds' ~ '^[0-9]+$'
                      THEN (payload->>'milliseconds')::numeric END)::text AS average_visible_ms,
             MIN(occurred_at) AS first_seen_at
           FROM section_events
           GROUP BY payload->>'sectionId'
           ORDER BY first_seen_at`,
          [ownerId(request), params.data.landingId, interval],
        ),
      ]);
      const row = summaryResult.rows[0];
      const sessions = Number(row?.sessions ?? 0);
      const pageViews = Number(row?.page_views ?? 0);
      const formSubmits = Number(row?.form_submits ?? 0);
      await reply.send({
        range: queryObject.data.range,
        sessions,
        pageViews,
        ctaClicks: Number(row?.cta_clicks ?? 0),
        formSubmits,
        conversionRate: sessions > 0 ? formSubmits / sessions : 0,
        rageClicks: Number(row?.rage_clicks ?? 0),
        deadClicks: Number(row?.dead_clicks ?? 0),
        averageDurationMs: Number(row?.average_duration_ms ?? 0),
        averageMaxScroll: Number(row?.average_max_scroll ?? 0),
        visits,
        ctaPct: pct(ctaSessions),
        funnel,
        sections: sectionsResult.rows.map((section) => {
          const reachedVisitors = Number(section.reached_sessions);
          return {
            sectionId: section.section_id,
            reachedVisitors,
            reachRate: sessions > 0 ? (reachedVisitors / sessions) * 100 : 0,
            averageVisibleMs: section.average_visible_ms === null
              ? null
              : Number(section.average_visible_ms),
          };
        }),
      });
    },
  );

  app.get(
    `${API_PREFIX}/landings/:landingId/sessions`,
    { preHandler: auth.authenticate.bind(auth) },
    async (request, reply) => {
      const params = landingParamsSchema.safeParse(request.params);
      const query = sessionsQuerySchema.safeParse(request.query);
      if (!params.success || !query.success) {
        await reply.code(400).send({ error: 'invalid_request' });
        return;
      }
      let cursor: z.infer<typeof cursorSchema> | null;
      try {
        cursor = parseCursor(query.data.cursor);
      } catch {
        await reply.code(400).send({ error: 'invalid_cursor' });
        return;
      }

      const result = await database.query<SessionRow>(
        `SELECT s.deployment_id, s.session_id, s.landing_id, s.started_at,
                s.last_seen_at, s.ended_at, s.page_path, s.context,
                (EXTRACT(EPOCH FROM (COALESCE(s.ended_at, s.last_seen_at) - s.started_at)) * 1000)::text
                  AS duration_ms,
                s.context->>'device' AS device,
                stats.max_scroll_depth,
                COALESCE(stats.cta_clicks, 0)::text AS cta_clicks,
                COALESCE(stats.form_submissions, 0)::text AS form_submissions,
                s.replay_event_count,
                EXISTS (
                  SELECT 1 FROM insights_replay_chunks r
                  WHERE r.deployment_id = s.deployment_id AND r.session_id = s.session_id
                ) AS replay_available
         FROM insights_sessions s
         LEFT JOIN LATERAL (
           SELECT
             MAX(CASE WHEN event_type = 'max_scroll'
                      AND payload->>'percent' ~ '^[0-9]+(\\.[0-9]+)?$'
                      THEN (payload->>'percent')::numeric END)::text AS max_scroll_depth,
             COUNT(*) FILTER (WHERE event_type = 'cta_click') AS cta_clicks,
             COUNT(*) FILTER (WHERE event_type = 'form_submit') AS form_submissions
           FROM insights_analytics_events e
           WHERE e.owner_id = s.owner_id
             AND e.deployment_id = s.deployment_id
             AND e.session_id = s.session_id
         ) stats ON true
         WHERE s.owner_id = $1
           AND s.landing_id = $2
           AND EXISTS (
             SELECT 1 FROM insights_replay_chunks available_replay
             WHERE available_replay.deployment_id = s.deployment_id
               AND available_replay.session_id = s.session_id
           )
           AND ($3::timestamptz IS NULL OR (s.started_at, s.deployment_id, s.session_id) <
               ($3::timestamptz, $4::uuid, $5::uuid))
         ORDER BY s.started_at DESC, s.deployment_id DESC, s.session_id DESC
         LIMIT $6`,
        [
          ownerId(request),
          params.data.landingId,
          cursor?.startedAt ?? null,
          cursor?.deploymentId ?? null,
          cursor?.sessionId ?? null,
          query.data.limit + 1,
        ],
      );
      const hasMore = result.rows.length > query.data.limit;
      const items = result.rows.slice(0, query.data.limit);
      const last = items.at(-1);
      await reply.send({
        items: items.map((row) => ({
          sessionId: row.session_id,
          landingId: row.landing_id,
          startedAt: row.started_at,
          durationMs: Number(row.duration_ms ?? 0),
          device: row.device,
          maxScrollDepth: row.max_scroll_depth === null ? null : Number(row.max_scroll_depth),
          ctaClicks: Number(row.cta_clicks),
          formSubmissions: Number(row.form_submissions),
          replayEventCount: row.replay_event_count,
          replayAvailable: row.replay_available,
        })),
        nextCursor: hasMore && last ? encodeCursor(last) : null,
      });
    },
  );

  app.get(
    `${API_PREFIX}/landings/:landingId/sessions/:sessionId/events`,
    { preHandler: auth.authenticate.bind(auth) },
    async (request, reply) => {
      const params = landingSessionParamsSchema.safeParse(request.params);
      if (!params.success) {
        await reply.code(400).send({ error: 'invalid_request' });
        return;
      }
      const session = await database.query<SessionRow>(
        `SELECT deployment_id, session_id, landing_id, started_at, last_seen_at,
                ended_at, page_path, context, replay_event_count, false AS replay_available
         FROM insights_sessions
         WHERE owner_id = $1 AND landing_id = $2 AND session_id = $3
         ORDER BY started_at DESC
         LIMIT 1`,
        [ownerId(request), params.data.landingId, params.data.sessionId],
      );
      const selected = session.rows[0];
      if (!selected) {
        await reply.code(404).send({ error: 'not_found' });
        return;
      }
      const [analytics, replayFiles] = await Promise.all([
        database.query<AnalyticsRow>(
          `SELECT event_type AS type, occurred_at AS at, payload AS data
           FROM insights_analytics_events
           WHERE owner_id = $1 AND deployment_id = $2 AND session_id = $3
           ORDER BY occurred_at, id`,
          [ownerId(request), selected.deployment_id, selected.session_id],
        ),
        database.query<ReplayFileRow>(
          `SELECT r.file_name
           FROM insights_replay_chunks r
           JOIN insights_sessions s
             ON s.deployment_id = r.deployment_id AND s.session_id = r.session_id
           WHERE s.owner_id = $1 AND r.deployment_id = $2 AND r.session_id = $3
           ORDER BY r.sequence`,
          [ownerId(request), selected.deployment_id, selected.session_id],
        ),
      ]);
      const replayEvents: unknown[] = [];
      for (const file of replayFiles.rows) {
        replayEvents.push(...(await replayStore.read(file.file_name)));
      }
      sortReplayEvents(replayEvents);
      await reply.send({
        sessionId: selected.session_id,
        analytics: analytics.rows.map((event) => ({
          type: event.type,
          at: event.at,
          data: event.data,
        })),
        replayEvents,
        replayAvailable: replayFiles.rows.length > 0,
      });
    },
  );
}
