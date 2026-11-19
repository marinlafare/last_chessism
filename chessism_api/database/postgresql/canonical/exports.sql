-- Credentials and durable job ownership. Requests/results remain bounded files.
CREATE TABLE export_client (
    id varchar(32) PRIMARY KEY,
    name varchar(100) NOT NULL, token_hash varchar(64) NOT NULL UNIQUE,
    allowed_sources varchar(40)[] NOT NULL, max_rows integer NOT NULL CHECK(max_rows>0),
    allow_analytics boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz NOT NULL,
    revoked_at timestamptz
);
CREATE TABLE export_job (
    id varchar(32) PRIMARY KEY,
    client_id varchar(32) NOT NULL REFERENCES export_client(id),
    idempotency_key varchar(128) NOT NULL, request_sha256 varchar(64) NOT NULL,
    name varchar(100) NOT NULL, source varchar(40) NOT NULL,
    row_limit integer NOT NULL CHECK(row_limit>0),
    status varchar(20) NOT NULL DEFAULT 'queued', phase varchar(32) NOT NULL DEFAULT 'queued',
    rows_written bigint NOT NULL DEFAULT 0 CHECK(rows_written>=0),
    file_bytes bigint NOT NULL DEFAULT 0 CHECK(file_bytes>=0), file_sha256 varchar(64),
    limit_reached boolean, cancel_requested boolean NOT NULL DEFAULT false,
    error varchar(500), error_code varchar(64),
    created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz, snapshot_at timestamptz, completed_at timestamptz, expires_at timestamptz,
    UNIQUE(client_id,idempotency_key)
);
CREATE INDEX ix_export_job_client_id ON export_job(client_id);
CREATE INDEX ix_export_job_status ON export_job(status);
CREATE INDEX ix_export_job_expires_at ON export_job(expires_at);
