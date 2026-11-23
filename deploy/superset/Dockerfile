FROM apache/superset:6.0.0

USER root
# PostgreSQL driver for the metadata DB and for the streamguard data source.
RUN . /app/.venv/bin/activate \
    && uv pip install --no-cache psycopg2-binary==2.9.10
USER superset
