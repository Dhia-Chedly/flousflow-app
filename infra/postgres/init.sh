#!/bin/sh
set -eu
: "${FLOUSFLOW_POSTGRES_LOADER_PASSWORD:?Set loader password}"
: "${FLOUSFLOW_POSTGRES_READER_PASSWORD:?Set reader password}"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=loader_password="$FLOUSFLOW_POSTGRES_LOADER_PASSWORD" \
  --set=reader_password="$FLOUSFLOW_POSTGRES_READER_PASSWORD" <<'SQL'
CREATE ROLE flousflow_loader LOGIN PASSWORD :'loader_password';
CREATE ROLE flousflow_reader LOGIN PASSWORD :'reader_password';
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA serving;
CREATE TABLE serving.hourly_transaction_metrics (
 replay_id text NOT NULL, hour timestamptz NOT NULL, transaction_type text NOT NULL,
 transaction_count bigint NOT NULL CHECK (transaction_count > 0),
 total_amount numeric(38,2) NOT NULL CHECK (total_amount >= 0),
 average_amount numeric(38,6) NOT NULL CHECK (average_amount >= 0),
 fraud_count bigint NOT NULL CHECK (fraud_count BETWEEN 0 AND transaction_count),
 fraud_rate double precision NOT NULL CHECK (fraud_rate BETWEEN 0 AND 1),
 PRIMARY KEY (replay_id, hour, transaction_type)
);
CREATE TABLE serving.account_activity (
 replay_id text NOT NULL, account_id text NOT NULL,
 incoming_count bigint NOT NULL CHECK (incoming_count >= 0),
 incoming_amount numeric(38,2) NOT NULL CHECK (incoming_amount >= 0),
 incoming_fraud_count bigint NOT NULL CHECK (incoming_fraud_count BETWEEN 0 AND incoming_count),
 outgoing_count bigint NOT NULL CHECK (outgoing_count >= 0),
 outgoing_amount numeric(38,2) NOT NULL CHECK (outgoing_amount >= 0),
 outgoing_fraud_count bigint NOT NULL CHECK (outgoing_fraud_count BETWEEN 0 AND outgoing_count),
 first_activity_at timestamptz NOT NULL, last_activity_at timestamptz NOT NULL,
 CHECK (first_activity_at <= last_activity_at), PRIMARY KEY (replay_id, account_id)
);
CREATE TABLE serving.refresh_status (
 id boolean PRIMARY KEY DEFAULT true CHECK (id), refreshed_at timestamptz NOT NULL,
 silver_path text NOT NULL, silver_snapshot bigint NOT NULL,
 hourly_snapshot bigint NOT NULL, accounts_snapshot bigint NOT NULL,
 hourly_rows bigint NOT NULL, account_rows bigint NOT NULL
);
GRANT USAGE ON SCHEMA serving TO flousflow_loader, flousflow_reader;
GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA serving TO flousflow_loader;
GRANT SELECT ON ALL TABLES IN SCHEMA serving TO flousflow_reader;
SQL
