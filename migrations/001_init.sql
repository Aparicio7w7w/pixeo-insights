CREATE TABLE IF NOT EXISTS insights_deployments (
  id uuid PRIMARY KEY,
  owner_id text NOT NULL,
  landing_id varchar(128) NOT NULL,
  landing_name varchar(200) NOT NULL,
  store_id varchar(128),
  shop_domain varchar(253) NOT NULL,
  product_id varchar(128) NOT NULL,
  token_hash char(64) NOT NULL UNIQUE,
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  deactivated_at timestamptz
);

CREATE INDEX IF NOT EXISTS insights_deployments_owner_landing
  ON insights_deployments (owner_id, landing_id, created_at DESC);

CREATE TABLE IF NOT EXISTS insights_sessions (
  deployment_id uuid NOT NULL REFERENCES insights_deployments(id) ON DELETE CASCADE,
  session_id uuid NOT NULL,
  owner_id text NOT NULL,
  landing_id varchar(128) NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  ended_at timestamptz,
  page_path varchar(2048) NOT NULL,
  context jsonb NOT NULL DEFAULT '{}'::jsonb,
  replay_enabled boolean NOT NULL DEFAULT false,
  replay_raw_bytes integer NOT NULL DEFAULT 0,
  replay_event_count integer NOT NULL DEFAULT 0,
  PRIMARY KEY (deployment_id, session_id)
);

CREATE INDEX IF NOT EXISTS insights_sessions_owner_started
  ON insights_sessions (owner_id, started_at DESC, deployment_id, session_id);
CREATE INDEX IF NOT EXISTS insights_sessions_owner_landing_started
  ON insights_sessions (owner_id, landing_id, started_at DESC);

CREATE TABLE IF NOT EXISTS insights_ingest_batches (
  deployment_id uuid NOT NULL,
  session_id uuid NOT NULL,
  sequence integer NOT NULL,
  kind varchar(20) NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_id, session_id, sequence, kind),
  FOREIGN KEY (deployment_id, session_id)
    REFERENCES insights_sessions(deployment_id, session_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS insights_analytics_events (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  deployment_id uuid NOT NULL,
  session_id uuid NOT NULL,
  owner_id text NOT NULL,
  landing_id varchar(128) NOT NULL,
  batch_sequence integer NOT NULL,
  event_type varchar(40) NOT NULL,
  occurred_at timestamptz NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (deployment_id, session_id)
    REFERENCES insights_sessions(deployment_id, session_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS insights_events_owner_landing_time
  ON insights_analytics_events (owner_id, landing_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS insights_events_session_time
  ON insights_analytics_events (deployment_id, session_id, occurred_at);

CREATE TABLE IF NOT EXISTS insights_replay_chunks (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  deployment_id uuid NOT NULL,
  session_id uuid NOT NULL,
  sequence integer NOT NULL,
  file_name varchar(255) NOT NULL UNIQUE,
  byte_size integer NOT NULL,
  raw_bytes integer NOT NULL,
  event_count integer NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (deployment_id, session_id, sequence),
  FOREIGN KEY (deployment_id, session_id)
    REFERENCES insights_sessions(deployment_id, session_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS insights_replay_chunks_created
  ON insights_replay_chunks (created_at);
