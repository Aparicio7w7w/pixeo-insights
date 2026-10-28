import { buildApp } from './app.js';
import { loadConfig } from './config.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const runtime = await buildApp(config);
  const cleanupTimer = setInterval(() => {
    void runtime.collector.cleanupExpiredReplays().catch((error: unknown) => {
      runtime.app.log.error({ err: error }, 'replay_cleanup_failed');
    });
  }, config.cleanupIntervalMinutes * 60_000);
  cleanupTimer.unref();

  const close = async (signal: string): Promise<void> => {
    runtime.app.log.info({ signal }, 'shutdown_started');
    clearInterval(cleanupTimer);
    await runtime.app.close();
    await runtime.database.close();
  };
  process.once('SIGTERM', () => void close('SIGTERM'));
  process.once('SIGINT', () => void close('SIGINT'));

  await runtime.app.listen({ host: config.host, port: config.port });
  runtime.app.log.info({ port: config.port }, 'pixeo_insights_started');
}

main().catch((error: unknown) => {
  process.stderr.write(`pixeo-insights failed to start: ${error instanceof Error ? error.message : 'unknown'}\n`);
  process.exitCode = 1;
});
