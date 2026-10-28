import { readFile, statfs } from 'node:fs/promises';
import { totalmem, freemem } from 'node:os';

import type { AppConfig } from './config.js';
import type { Queryable } from './database.js';

interface QuotaRow {
  used_bytes: string;
}

async function readNumber(path: string): Promise<number | null> {
  try {
    const value = (await readFile(path, 'utf8')).trim();
    if (value === 'max') return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

async function freeMemoryRatio(): Promise<number> {
  const [limit, current] = await Promise.all([
    readNumber('/sys/fs/cgroup/memory.max'),
    readNumber('/sys/fs/cgroup/memory.current'),
  ]);

  if (limit !== null && current !== null && limit > 0) {
    return Math.max(0, limit - current) / limit;
  }

  return freemem() / totalmem();
}

export class ReplayGate {
  public constructor(
    private readonly config: AppConfig,
    private readonly database: Queryable,
  ) {}

  public async canRecord(): Promise<boolean> {
    if (Math.random() >= this.config.replaySampleRate) return false;

    return this.hasCapacity();
  }

  public async hasCapacity(): Promise<boolean> {
    try {
      const [filesystem, memoryRatio, quotaResult] = await Promise.all([
        statfs(this.config.replayDir),
        freeMemoryRatio(),
        this.database.query<QuotaRow>(
          'SELECT COALESCE(SUM(byte_size), 0)::text AS used_bytes FROM insights_replay_chunks',
        ),
      ]);
      const freeDiskBytes = Number(filesystem.bavail) * Number(filesystem.bsize);
      const usedBytes = Number(quotaResult.rows[0]?.used_bytes ?? '0');

      return (
        freeDiskBytes >= this.config.replayMinFreeDiskBytes &&
        memoryRatio >= this.config.replayMinFreeMemoryRatio &&
        usedBytes < this.config.replayQuotaBytes
      );
    } catch {
      return false;
    }
  }
}
