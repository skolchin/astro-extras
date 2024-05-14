# Astro SDK Extras project
# (c) kol, 2023

""" DAGs for Astro SDK capanilities demo """

import pendulum
import pandas as pd
from airflow.models import DAG
from astro.sql.table import Table, Metadata
from astro_extras import compare_tables

with DAG(
    dag_id='compare-tables',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
    tags=['demo', 'astro-extras'],
) as dag:

    ora_table = Table('types', conn_id='source_db')
    psg_table = Table('types', conn_id='target_db', metadata=Metadata(schema='stage'))

    compare_tables([ora_table], [psg_table])
