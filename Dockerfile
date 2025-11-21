FROM apache/airflow:2.7.1
USER airflow
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary FlightRadarAPI beautifulsoup4
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary pyarrow requests