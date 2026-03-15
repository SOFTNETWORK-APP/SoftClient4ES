FROM python:3.12-slim

RUN pip install --no-cache-dir \
    duckdb \
    adbc-driver-flightsql \
    adbc-driver-manager \
    pyarrow \
    tabulate

WORKDIR /demo

COPY demo.py /demo/demo.py

ENTRYPOINT ["python", "/demo/demo.py"]
