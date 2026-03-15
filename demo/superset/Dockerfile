FROM apache/superset:latest

USER root

COPY elastic_dialect.py /tmp/dialect/elastic_dialect.py
COPY setup_dialect.py /tmp/dialect/setup.py

RUN /app/.venv/bin/python -m ensurepip && \
    /app/.venv/bin/python -m pip install --no-cache-dir \
    flightsql-dbapi \
    psycopg2-binary && \
    /app/.venv/bin/python -m pip install --no-cache-dir /tmp/dialect/

USER superset

COPY superset_config.py /app/pythonpath/superset_config.py
