-- Required saved recipes, job outcomes and artifact metadata. Preserve the existing
-- public research contracts; only the empty canonical initializer owns this DDL.
CREATE TABLE matrix_definition (
    id varchar(36) PRIMARY KEY, name varchar(100) NOT NULL, row_type varchar(32) NOT NULL,
    config json NOT NULL, created_by varchar(36) REFERENCES account(id) ON DELETE SET NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_matrix_definition_created_at ON matrix_definition(created_at DESC);
CREATE TABLE algorithm_definition (
    id varchar(36) PRIMARY KEY, name varchar(100) NOT NULL,
    matrix_definition_id varchar(36) REFERENCES matrix_definition(id) ON DELETE SET NULL,
    config json NOT NULL, created_by varchar(36) REFERENCES account(id) ON DELETE SET NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_algorithm_definition_created_at ON algorithm_definition(created_at DESC);
CREATE TABLE algorithm_run (
    id varchar(36) PRIMARY KEY,
    definition_id varchar(36) REFERENCES algorithm_definition(id) ON DELETE SET NULL,
    name varchar(100) NOT NULL, config json NOT NULL,
    status varchar(16) NOT NULL CHECK(status IN ('queued','running','complete','failed','cancelled')),
    cancel_requested boolean NOT NULL, progress json NOT NULL, result json, error varchar(2000),
    created_by varchar(36) REFERENCES account(id) ON DELETE SET NULL,
    created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, finished_at timestamptz
);
CREATE INDEX ix_algorithm_run_created_at ON algorithm_run(created_at DESC);
CREATE INDEX ix_algorithm_run_active ON algorithm_run(status) WHERE status IN ('queued','running');
CREATE TABLE matrix_artifact (
    id varchar(36) PRIMARY KEY, name varchar(100) NOT NULL, row_type varchar(32) NOT NULL,
    status varchar(16) NOT NULL CHECK(status IN ('queued','running','complete','failed')),
    job_id varchar(96), created_by varchar(36) REFERENCES account(id) ON DELETE SET NULL,
    config json NOT NULL, estimate json, result json, artifact_path varchar,
    row_count bigint, feature_count integer, label_count integer, size_bytes bigint, error varchar,
    created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, finished_at timestamptz
);
CREATE INDEX ix_matrix_artifact_row_type ON matrix_artifact(row_type);
CREATE INDEX ix_matrix_artifact_status ON matrix_artifact(status);
CREATE INDEX ix_matrix_artifact_job_id ON matrix_artifact(job_id);
CREATE INDEX ix_matrix_artifact_created_at ON matrix_artifact(created_at DESC);

-- Optional catalogue contents; an empty initialized table is a valid lookup.
CREATE TABLE opening_book_move (
    position_key text NOT NULL,
    move_uci text NOT NULL CHECK(move_uci ~ '^[a-h][1-8][a-h][1-8][qrbn]?$'),
    result_position_key text NOT NULL,
    opening_labels jsonb NOT NULL CHECK(jsonb_typeof(opening_labels)='array'),
    source_revision text NOT NULL CHECK(source_revision ~ '^[0-9a-f]{40}$'),
    importer_version text NOT NULL, imported_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(position_key,move_uci)
);
COMMENT ON TABLE opening_book_move IS 'Lichess CC0 opening catalogue: recognized position/move transitions, not engine assessments or frequency counts';

-- Existing coefficient-research API storage; no changes to production scoring.
CREATE TABLE coefficient_research_experiment (
    id varchar(36) PRIMARY KEY,
    status varchar(16) NOT NULL CHECK(status IN ('queued','running','complete','failed')),
    job_id varchar(64), created_by varchar(36) REFERENCES account(id) ON DELETE SET NULL,
    config json NOT NULL, dataset_summary json, result json, error varchar,
    decision varchar(32) CHECK(decision IS NULL OR decision IN ('keep_lichess','alongside','replace')),
    decision_config json, decision_notes varchar,
    created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz,
    finished_at timestamptz, decided_at timestamptz
);
CREATE INDEX ix_coefficient_research_experiment_status ON coefficient_research_experiment(status);
CREATE INDEX ix_coefficient_research_experiment_job_id ON coefficient_research_experiment(job_id);
