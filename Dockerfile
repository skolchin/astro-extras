FROM apache/airflow:slim-2.6.1-python3.10
COPY ./docker/requirements-docker.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY ./dist/astro_extras-0.0.1-py3-none-any.whl /tmp
RUN pip install --no-cache-dir /tmp/*.whl
