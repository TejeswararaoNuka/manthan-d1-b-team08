-- Runs once, on an empty data volume, as POSTGRES_USER via psql (ON_ERROR_STOP is on).
-- Passwords are read from the container environment (../.env), never written here.

\getenv ro_pw SUPERSET_RO_PASSWORD
\getenv fw_pw FEEDBACK_WRITER_PASSWORD
\getenv app_db POSTGRES_DB

\if :{?ro_pw}
\else
    DO $$ BEGIN RAISE EXCEPTION 'SUPERSET_RO_PASSWORD is not set in .env'; END $$;
\endif

\if :{?fw_pw}
\else
    DO $$ BEGIN RAISE EXCEPTION 'FEEDBACK_WRITER_PASSWORD is not set in .env'; END $$;
\endif

-- superset_ro: Superset's connection to the streamguard database. Read-only.
CREATE ROLE superset_ro LOGIN PASSWORD :'ro_pw';

-- feedback_writer: the Streamlit feedback app (D-05).
-- Table grants (SELECT on alerts, INSERT/UPDATE on alert_feedback) are added in sql/ddl
-- by the file that creates those tables, because they do not exist yet.
CREATE ROLE feedback_writer LOGIN PASSWORD :'fw_pw';

GRANT CONNECT ON DATABASE :"app_db" TO superset_ro, feedback_writer;

\connect :"app_db"

GRANT USAGE ON SCHEMA public TO superset_ro, feedback_writer;

-- Read everything that exists now, and every table POSTGRES_USER creates later.
-- (Default privileges apply to objects created by the role running this script, which is
-- also the role that runs the DDL in sql/ddl.)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO superset_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO superset_ro;
