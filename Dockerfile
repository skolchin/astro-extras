ARG REG_PROXY
ARG AIRFLOW_UID=50000
FROM ${REG_PROXY}apache/airflow:slim-2.8.4-python3.10

USER 0
RUN apt-get update && apt-get install -y nano gosu
RUN apt-get install -y locales \
    && localedef -i ru_RU -c -f UTF-8 -A /usr/share/locale/locale.alias ru_RU.UTF-8
RUN ln -sf /usr/share/zoneinfo/W-SU /etc/localtime

USER $AIRFLOW_UID
COPY ./docker/requirements-docker.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN mkdir /tmp/astro_extras

COPY ./setup.py /tmp/astro_extras/
COPY ./MANIFEST.in /tmp/astro_extras/
COPY ./pyproject.toml /tmp/astro_extras/
COPY ./requirements.txt /tmp/astro_extras/
COPY ./README.md /tmp/astro_extras/
COPY ./src /tmp/astro_extras/src
RUN pip install -e /tmp/astro_extras

# this is to debug openlineage integration
# COPY ./docker/ol_base_patched.py /home/airflow/.local/lib/python3.10/site-packages/airflow/providers/openlineage/extractors/base.py

USER 0
COPY ./docker/airflow.cfg /opt/airflow/airflow.cfg
RUN chown -R $AIRFLOW_UID:root /opt/airflow/
