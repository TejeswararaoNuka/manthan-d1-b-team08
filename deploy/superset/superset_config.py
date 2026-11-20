"""Superset config for StreamGuard. Mounted at /app/pythonpath/superset_config.py.

All secrets come from environment variables (../.env), never from this file.
"""

import os

from sqlalchemy.engine import URL

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Metadata DB: the "superset" database in the postgres service (Docker network name).
# URL.create escapes special characters in the password.
SQLALCHEMY_DATABASE_URI = URL.create(
    drivername="postgresql+psycopg2",
    username=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    host="postgres",
    port=5432,
    database="superset",
).render_as_string(hide_password=False)

# Analysts read only; feedback goes through a separate feedback_writer connection (D-05).
# DML stays disabled on data sources (Superset's default allow_dml=False).
