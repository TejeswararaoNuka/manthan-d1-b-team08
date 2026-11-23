-- Runs once, on an empty data volume, as POSTGRES_USER via psql.
-- POSTGRES_DB (streamguard) is already created by the postgres image entrypoint.

\getenv test_db POSTGRES_TEST_DB

\if :{?test_db}
\else
    \set test_db streamguard_test
\endif

-- Superset metadata database (users, dashboards, saved queries).
CREATE DATABASE superset;

-- Test database: tests use POSTGRES_TEST_DB, never POSTGRES_DB.
CREATE DATABASE :"test_db";

-- Nobody connects to these by default; only POSTGRES_USER (owner) can.
REVOKE CONNECT ON DATABASE superset FROM PUBLIC;
REVOKE CONNECT ON DATABASE :"test_db" FROM PUBLIC;
