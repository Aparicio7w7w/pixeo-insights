import { createHash } from 'node:crypto';

export function sha256(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

export function normalizeShopDomain(value: string): string {
  const candidate = /^https?:\/\//i.test(value) ? value : `https://${value}`;
  const url = new URL(candidate);
  const hostname = url.hostname.toLowerCase().replace(/\.$/, '');

  if (
    !hostname ||
    hostname.length > 253 ||
    hostname === 'localhost' ||
    hostname.includes('..') ||
    !hostname.includes('.') ||
    !/^[a-z0-9.-]+$/.test(hostname)
  ) {
    throw new Error('shopDomain inválido');
  }

  return hostname;
}

export function isCompatibleOrigin(origin: string | undefined, shopDomain: string): boolean {
  if (!origin) return false;

  try {
    const hostname = new URL(origin).hostname.toLowerCase().replace(/\.$/, '');
    const normalizedShop = shopDomain.replace(/^www\./, '');
    const normalizedHost = hostname.replace(/^www\./, '');
    return normalizedHost === normalizedShop || normalizedHost.endsWith(`.${normalizedShop}`);
  } catch {
    return false;
  }
}

export function safeOccurredAt(value: number): Date {
  const now = Date.now();
  const minimum = now - 30 * 24 * 60 * 60 * 1000;
  const maximum = now + 5 * 60 * 1000;
  return new Date(Math.min(maximum, Math.max(minimum, value)));
}
