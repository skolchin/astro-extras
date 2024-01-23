FROM apache/airflow:slim-2.7.1-python3.10
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

USER 0
RUN ln -sf /usr/share/zoneinfo/W-SU /etc/localtime
COPY ./docker/airflow.cfg /opt/airflow/airflow.cfg
RUN chown -R $AIRFLOW_UID:root /opt/airflow/
