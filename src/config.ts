import { z } from 'zod';

const booleanFromString = z
  .enum(['true', 'false'])
  .default('false')
  .transform((value) => value === 'true');

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('production'),
  HOST: z.string().min(1).default('0.0.0.0'),
  PORT: z.coerce.number().int().min(1).max(65_535).default(3000),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),
  DATABASE_URL: z.url(),
  SUPABASE_URL: z.url(),
  SUPABASE_ISSUER: z.url().optional(),
  SUPABASE_AUDIENCE: z.string().min(1).default('authenticated'),
  PUBLIC_BASE_URL: z.url(),
  DASHBOARD_ORIGINS: z.string().min(1),
  TRUST_PROXY: booleanFromString,
  REPLAY_DIR: z.string().min(1).default('/data/replays'),
  REPLAY_SAMPLE_RATE: z.coerce.number().min(0).max(1).default(0.05),
  REPLAY_MAX_SESSION_BYTES: z.coerce.number().int().min(1).default(3_145_728),
  REPLAY_MIN_FREE_DISK_BYTES: z.coerce.number().int().min(0).default(16_106_127_360),
  REPLAY_MIN_FREE_MEMORY_RATIO: z.coerce.number().min(0).max(1).default(0.15),
  REPLAY_QUOTA_BYTES: z.coerce.number().int().min(1).default(10_737_418_240),
  RETENTION_DAYS: z.coerce.number().int().min(1).max(365).default(7),
  CLEANUP_INTERVAL_MINUTES: z.coerce.number().int().min(1).default(60),
  COLLECT_RATE_LIMIT_PER_MINUTE: z.coerce.number().int().min(1).default(120),
  COLLECT_BODY_LIMIT_BYTES: z.coerce.number().int().min(1024).default(6_553_600),
});

export interface AppConfig {
  nodeEnv: 'development' | 'test' | 'production';
  host: string;
  port: number;
  logLevel: string;
  databaseUrl: string;
  supabaseUrl: string;
  supabaseIssuer: string;
  supabaseAudience: string;
  publicBaseUrl: string;
  dashboardOrigins: ReadonlySet<string>;
  trustProxy: boolean;
  replayDir: string;
  replaySampleRate: number;
  replayMaxSessionBytes: number;
  replayMinFreeDiskBytes: number;
  replayMinFreeMemoryRatio: number;
  replayQuotaBytes: number;
  retentionDays: number;
  cleanupIntervalMinutes: number;
  collectRateLimitPerMinute: number;
  collectBodyLimitBytes: number;
}

function normalizeOrigin(value: string): string {
  const url = new URL(value.trim());
  return url.origin;
}

export function loadConfig(environment: NodeJS.ProcessEnv = process.env): AppConfig {
  const env = envSchema.parse(environment);
  // Mínimo 3MB por sesión: los FullSnapshot de Shopify superan 500KB.
  const replayMaxSessionBytes = Math.max(env.REPLAY_MAX_SESSION_BYTES, 3_145_728);
  const supabaseUrl = env.SUPABASE_URL.replace(/\/$/, '');
  const dashboardOrigins = new Set(
    env.DASHBOARD_ORIGINS.split(',').map(normalizeOrigin),
  );

  return {
    nodeEnv: env.NODE_ENV,
    host: env.HOST,
    port: env.PORT,
    logLevel: env.LOG_LEVEL,
    databaseUrl: env.DATABASE_URL,
    supabaseUrl,
    supabaseIssuer: env.SUPABASE_ISSUER ?? `${supabaseUrl}/auth/v1`,
    supabaseAudience: env.SUPABASE_AUDIENCE,
    publicBaseUrl: env.PUBLIC_BASE_URL.replace(/\/$/, ''),
    dashboardOrigins,
    trustProxy: env.TRUST_PROXY,
    replayDir: env.REPLAY_DIR,
    replaySampleRate: env.REPLAY_SAMPLE_RATE,
    replayMaxSessionBytes,
    replayMinFreeDiskBytes: env.REPLAY_MIN_FREE_DISK_BYTES,
    replayMinFreeMemoryRatio: env.REPLAY_MIN_FREE_MEMORY_RATIO,
    replayQuotaBytes: env.REPLAY_QUOTA_BYTES,
    retentionDays: env.RETENTION_DAYS,
    cleanupIntervalMinutes: env.CLEANUP_INTERVAL_MINUTES,
    collectRateLimitPerMinute: env.COLLECT_RATE_LIMIT_PER_MINUTE,
    collectBodyLimitBytes: Math.max(env.COLLECT_BODY_LIMIT_BYTES, replayMaxSessionBytes * 2 + 262_144),
  };
}
