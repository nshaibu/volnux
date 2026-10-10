-- deploy/docker/postgres-init/01-init.sql
-- Runs once on first PostgreSQL container startup (docker-compose).
-- Equivalent to 04-postgres.yaml initContainers in Kubernetes.

-- Extensions required by Volnux
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- Database-level settings
ALTER DATABASE volnux SET timezone            = 'UTC';
ALTER DATABASE volnux SET statement_timeout   = '300s';
ALTER DATABASE volnux SET idle_in_transaction_session_timeout = '60s';
