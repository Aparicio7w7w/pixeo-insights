import { z } from 'zod';

export const safeIdSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[A-Za-z0-9][A-Za-z0-9._:-]*$/);

export const deploymentBodySchema = z
  .object({
    landingId: safeIdSchema,
    landingName: z.string().trim().min(1).max(200),
    storeId: safeIdSchema.optional(),
    shopDomain: z.string().trim().min(1).max(253),
    productId: z.coerce.string().trim().min(1).max(255),
  })
  .strict();

const sessionContextSchema = z
  .object({
    referrerHost: z.string().max(253).default(''),
    device: z.enum(['mobile', 'tablet', 'desktop', 'unknown']),
    viewportWidth: z.number().int().min(0).max(20_000),
    viewportHeight: z.number().int().min(0).max(20_000),
    language: z.string().max(35),
    utm: z
      .object({
        source: z.string().max(200).optional(),
        medium: z.string().max(200).optional(),
        campaign: z.string().max(200).optional(),
        content: z.string().max(200).optional(),
        term: z.string().max(200).optional(),
      })
      .strict(),
  })
  .strict();

const batchBaseSchema = z.object({
  version: z.literal(1),
  sessionId: z.uuid(),
  sequence: z.number().int().min(0).max(2_147_483_647),
  sentAt: z.number().int().positive(),
  pagePath: z.string().min(1).max(2048).regex(/^\//),
});

export const sessionStartBatchSchema = batchBaseSchema
  .extend({
    kind: z.literal('session_start'),
    context: sessionContextSchema,
  })
  .strict();

const scalarSchema = z.union([
  z.string().max(500),
  z.number(),
  z.boolean(),
  z.null(),
]);

const eventDataSchema = z.record(z.string().max(40), scalarSchema).superRefine((value, context) => {
  const entries = Object.entries(value);
  if (entries.length > 20) {
    context.addIssue({ code: 'custom', message: 'Demasiadas propiedades' });
  }

  for (const [key] of entries) {
    if (/value|email|phone|address|customer|password|text|input/i.test(key)) {
      context.addIssue({
        code: 'custom',
        message: 'Propiedad sensible no permitida',
        path: [key],
      });
    }
  }
});

export const analyticsEventTypes = [
  'page_view',
  'section_view',
  'visible_ms',
  'max_scroll',
  'cta_click',
  'form_seen',
  'form_submit',
  'rage_click',
  'dead_click',
  'page_end',
] as const;

const analyticsEventSchema = z
  .object({
    type: z.enum(analyticsEventTypes),
    at: z.number().int().positive(),
    data: eventDataSchema,
  })
  .strict();

export const analyticsBatchSchema = batchBaseSchema
  .extend({
    kind: z.literal('analytics'),
    events: z.array(analyticsEventSchema).min(1).max(100),
  })
  .strict();

const replayEventSchema = z
  .object({
    type: z.number().int().min(0),
    timestamp: z.number().int().positive(),
    data: z.unknown(),
  })
  .strict();

export const replayBatchSchema = batchBaseSchema
  .extend({
    kind: z.literal('replay'),
    events: z.array(replayEventSchema).min(1).max(500),
  })
  .strict();

export const collectorBatchSchema = z.discriminatedUnion('kind', [
  sessionStartBatchSchema,
  analyticsBatchSchema,
  replayBatchSchema,
]);

export type CollectorBatch = z.infer<typeof collectorBatchSchema>;
export type AnalyticsBatch = z.infer<typeof analyticsBatchSchema>;
export type ReplayBatch = z.infer<typeof replayBatchSchema>;

export const rangeSchema = z.enum(['24h', '7d', '30d']);

export const sessionsQuerySchema = z
  .object({
    cursor: z.string().max(1000).optional(),
    limit: z.coerce.number().int().min(1).max(100).default(50),
  })
  .strict();

export const landingParamsSchema = z.object({ landingId: safeIdSchema }).strict();
export const deploymentParamsSchema = z.object({ deploymentId: z.uuid() }).strict();
export const confirmDeploymentBodySchema = z
  .object({
    productHandle: z.string().trim().min(1).max(255),
    publishedUrl: z.url().max(2048),
  })
  .strict();
export const landingSessionParamsSchema = z
  .object({ landingId: safeIdSchema, sessionId: z.uuid() })
  .strict();
export const tokenParamsSchema = z
  .object({ token: z.string().regex(/^[A-Za-z0-9_-]{43}$/) })
  .strict();
