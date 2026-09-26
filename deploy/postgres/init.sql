-- Runs once, when the pgdata volume is first initialised, as the superuser
-- created from POSTGRES_USER (role "app", database "app"). The role and
-- database therefore already exist; only extensions are created here.
-- pg_stat_statements is preloaded through shared_preload_libraries in
-- docker-compose.yml; this statement exposes its view in the app database.
-- Server settings live in the compose command line, not in ALTER SYSTEM, so
-- a fresh volume always starts from the same configuration.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
