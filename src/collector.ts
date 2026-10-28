import { randomUUID } from 'node:crypto';

import type { PoolClient, QueryResultRow } from 'pg';

import type { AppConfig } from './config.js';
import type { Database } from './database.js';
import type { AnalyticsBatch, ReplayBatch } from './schemas.js';
import { ReplayGate } from './resources.js';
import { ReplayStore } from './replayStore.js';
import { safeOccurredAt } from './security.js';

export interface DeploymentRow extends QueryResultRow {
  id: string;
  owner_id: string;
  landing_id: string;
  shop_domain: string;
}

interface SessionReplayRow extends QueryResultRow {
  replay_enabled: boolean;
  replay_raw_bytes: number;
  started_at: Date;
}

interface FileRow extends QueryResultRow {
  id: string;
  file_name: string;
}

export interface CollectorResult {
  accepted: boolean;
  duplicate?: boolean;
  replay?: {
    enabled: boolean;
    maxDurationMs: number;
    maxBytes: number;
  };
  reason?: 'replay_disabled' | 'resource_limit' | 'session_limit';
}

export class CollectorService {
  public constructor(
    private readonly database: Database,
    private readonly replayStore: ReplayStore,
    private readonly replayGate: ReplayGate,
    private readonly config: AppConfig,
  ) {}

  public async findDeployment(tokenHash: string): Promise<DeploymentRow | null> {
    const result = await this.database.query<DeploymentRow>(
      `SELECT id, owner_id, landing_id, shop_domain
       FROM insights_deployments
       WHERE token_hash = $1 AND active = true`,
      [tokenHash],
    );
    return result.rows[0] ?? null;
  }

  public async startSession(
    deployment: DeploymentRow,
    batch: {
      sessionId: string;
      sequence: number;
      pagePath: string;
      context: Readonly<Record<string, unknown>>;
    },
  ): Promise<CollectorResult> {
    const replayEnabled = await this.replayGate.canRecord();

    return this.database.transaction(async (client) => {
      await client.query(
        `INSERT INTO insights_sessions (
           deployment_id, session_id, owner_id, landing_id, page_path, context, replay_enabled
         ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
         ON CONFLICT (deployment_id, session_id) DO UPDATE
         SET last_seen_at = now()`,
        [
          deployment.id,
          batch.sessionId,
          deployment.owner_id,
          deployment.landing_id,
          batch.pagePath,
          JSON.stringify(batch.context),
          replayEnabled,
        ],
      );
      const ingest = await client.query(
        `INSERT INTO insights_ingest_batches (deployment_id, session_id, sequence, kind)
         VALUES ($1, $2, $3, 'session_start')
         ON CONFLICT DO NOTHING
         RETURNING sequence`,
        [deployment.id, batch.sessionId, batch.sequence],
      );
      const session = await client.query<SessionReplayRow>(
        `SELECT replay_enabled, replay_raw_bytes, started_at
         FROM insights_sessions
         WHERE deployment_id = $1 AND session_id = $2`,
        [deployment.id, batch.sessionId],
      );

      return {
        accepted: true,
        duplicate: ingest.rowCount === 0,
        replay: {
          enabled: session.rows[0]?.replay_enabled ?? false,
          maxDurationMs: 180_000,
          maxBytes: this.config.replayMaxSessionBytes,
        },
      };
    });
  }

  public async ingestAnalytics(
    deployment: DeploymentRow,
    batch: AnalyticsBatch,
  ): Promise<CollectorResult> {
    return this.database.transaction(async (client) => {
      await this.ensureSession(client, deployment, batch.sessionId, batch.pagePath);
      const ingest = await client.query(
        `INSERT INTO insights_ingest_batches (deployment_id, session_id, sequence, kind)
         VALUES ($1, $2, $3, 'analytics')
         ON CONFLICT DO NOTHING
         RETURNING sequence`,
        [deployment.id, batch.sessionId, batch.sequence],
      );

      if (ingest.rowCount === 0) {
        return { accepted: true, duplicate: true };
      }

      const events = batch.events.map((event) => ({
        eventType: event.type,
        occurredAt: safeOccurredAt(event.at).toISOString(),
        payload: event.data,
      }));
      await client.query(
        `INSERT INTO insights_analytics_events (
           deployment_id, session_id, owner_id, landing_id, batch_sequence,
           event_type, occurred_at, payload
         )
         SELECT $1, $2, $3, $4, $5, event_type, occurred_at, payload
         FROM jsonb_to_recordset($6::jsonb) AS event_rows(
           event_type varchar(40), occurred_at timestamptz, payload jsonb
         )`,
        [
          deployment.id,
          batch.sessionId,
          deployment.owner_id,
          deployment.landing_id,
          batch.sequence,
          JSON.stringify(
            events.map((event) => ({
              event_type: event.eventType,
              occurred_at: event.occurredAt,
              payload: event.payload,
            })),
          ),
        ],
      );

      const ended = batch.events.some((event) => event.type === 'page_end');
      await client.query(
        `UPDATE insights_sessions
         SET last_seen_at = now(), ended_at = CASE WHEN $3 THEN now() ELSE ended_at END
         WHERE deployment_id = $1 AND session_id = $2`,
        [deployment.id, batch.sessionId, ended],
      );
      return { accepted: true };
    });
  }

  public async ingestReplay(
    deployment: DeploymentRow,
    batch: ReplayBatch,
  ): Promise<CollectorResult> {
    if (!(await this.replayGate.hasCapacity())) {
      return { accepted: false, reason: 'resource_limit' };
    }

    const rawBytes = Buffer.byteLength(JSON.stringify(batch.events));
    const writtenFile = `${deployment.id}_${batch.sessionId}_${String(batch.sequence)}_${randomUUID()}.json.gz`;

    try {
      return await this.database.transaction(async (client) => {
        const session = await client.query<SessionReplayRow>(
          `SELECT replay_enabled, replay_raw_bytes, started_at
           FROM insights_sessions
           WHERE deployment_id = $1 AND session_id = $2
           FOR UPDATE`,
          [deployment.id, batch.sessionId],
        );
        const current = session.rows[0];
        if (!current?.replay_enabled) {
          return { accepted: false, reason: 'replay_disabled' };
        }
        if (Date.now() - current.started_at.getTime() > 180_000) {
          return { accepted: false, reason: 'session_limit' };
        }
        // El snapshot inicial (Meta 4 + FullSnapshot 2) se acepta siempre: sin él el replay sale en blanco.
        const hasFullSnapshot = batch.events.some((event) => event.type === 2);
        if (
          !hasFullSnapshot &&
          current.replay_raw_bytes + rawBytes > this.config.replayMaxSessionBytes
        ) {
          return { accepted: false, reason: 'session_limit' };
        }

        const ingest = await client.query(
          `INSERT INTO insights_ingest_batches (deployment_id, session_id, sequence, kind)
           VALUES ($1, $2, $3, 'replay')
           ON CONFLICT DO NOTHING
           RETURNING sequence`,
          [deployment.id, batch.sessionId, batch.sequence],
        );
        if (ingest.rowCount === 0) {
          return { accepted: true, duplicate: true };
        }

        const stored = await this.replayStore.write(writtenFile, batch.events);
        await client.query(
          `INSERT INTO insights_replay_chunks (
             deployment_id, session_id, sequence, file_name, byte_size, raw_bytes, event_count
           ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
          [
            deployment.id,
            batch.sessionId,
            batch.sequence,
            writtenFile,
            stored.byteSize,
            stored.rawBytes,
            batch.events.length,
          ],
        );
        await client.query(
          `UPDATE insights_sessions
           SET last_seen_at = now(),
               replay_raw_bytes = replay_raw_bytes + $3,
               replay_event_count = replay_event_count + $4
           WHERE deployment_id = $1 AND session_id = $2`,
          [deployment.id, batch.sessionId, stored.rawBytes, batch.events.length],
        );
        return { accepted: true };
      });
    } catch (error: unknown) {
      await this.replayStore.remove(writtenFile);
      throw error;
    }
  }

  public async cleanupExpiredReplays(): Promise<number> {
    const cutoff = new Date(Date.now() - this.config.retentionDays * 86_400_000);
    const result = await this.database.query<FileRow>(
      `SELECT id::text, file_name
       FROM insights_replay_chunks
       WHERE created_at < $1
       ORDER BY created_at
       LIMIT 500`,
      [cutoff],
    );

    for (const row of result.rows) {
      await this.replayStore.remove(row.file_name);
      await this.database.query('DELETE FROM insights_replay_chunks WHERE id = $1', [row.id]);
    }
    return result.rowCount ?? 0;
  }

  private async ensureSession(
    client: PoolClient,
    deployment: DeploymentRow,
    sessionId: string,
    pagePath: string,
  ): Promise<void> {
    await client.query(
      `INSERT INTO insights_sessions (
         deployment_id, session_id, owner_id, landing_id, page_path, replay_enabled
       ) VALUES ($1, $2, $3, $4, $5, false)
       ON CONFLICT (deployment_id, session_id) DO UPDATE
       SET last_seen_at = now()`,
      [deployment.id, sessionId, deployment.owner_id, deployment.landing_id, pagePath],
    );
  }
}
