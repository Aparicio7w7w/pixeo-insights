import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';

import helmet from '@fastify/helmet';
import Fastify, { type FastifyInstance } from 'fastify';

import { SupabaseAuth } from './auth.js';
import { CollectorService } from './collector.js';
import type { AppConfig } from './config.js';
import { Database } from './database.js';
import { registerRoutes } from './routes.js';
import { ReplayGate } from './resources.js';
import { ReplayStore } from './replayStore.js';

export interface AppRuntime {
  app: FastifyInstance;
  collector: CollectorService;
  database: Database;
}

export async function buildApp(config: AppConfig): Promise<AppRuntime> {
  const app = Fastify({
    bodyLimit: config.collectBodyLimitBytes,
    disableRequestLogging: true,
    trustProxy: config.trustProxy,
    logger: {
      level: config.logLevel,
      redact: {
        paths: [
          'req.headers.authorization',
          'req.headers.cookie',
          'request.headers.authorization',
          'request.headers.cookie',
          '*.token',
        ],
        censor: '[REDACTED]',
      },
    },
  });
  await app.register(helmet, {
    contentSecurityPolicy: false,
    crossOriginResourcePolicy: { policy: 'cross-origin' },
  });
  app.addContentTypeParser(
    'text/plain',
    { parseAs: 'string', bodyLimit: config.collectBodyLimitBytes },
    (_request, body, done) => done(null, body),
  );
  app.decorateRequest('auth', null);

  const database = new Database(config.databaseUrl);
  const replayStore = new ReplayStore(config.replayDir);
  await replayStore.initialize();
  await database.migrate();

  const replayGate = new ReplayGate(config, database);
  const collector = new CollectorService(database, replayStore, replayGate, config);
  const auth = new SupabaseAuth(config);
  const trackerBundle = await readFile(resolve(process.cwd(), 'dist/public/tracker.js'));
  registerRoutes(app, { auth, collector, config, database, replayStore, trackerBundle });

  app.setErrorHandler(async (error, request, reply) => {
    request.log.error({ err: error, requestId: request.id }, 'request_failed');
    if (error instanceof Error && 'statusCode' in error && error.statusCode === 413) {
      await reply.code(413).send({ error: 'payload_too_large' });
      return;
    }
    await reply.code(500).send({ error: 'internal_error' });
  });
  app.setNotFoundHandler(async (_request, reply) => {
    await reply.code(404).send({ error: 'not_found' });
  });

  return { app, collector, database };
}
