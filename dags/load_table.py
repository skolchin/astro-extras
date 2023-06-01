# Astro SDK Extras project
# (c) kol, 2023

import pendulum
import pandas as pd
from airflow.models import DAG
from astro import sql as aql
from astro.sql.table import Table, Metadata

from astro_extras import load_table

with DAG(
    dag_id='load_table',
    start_date=pendulum.today('Europe/Moscow').add(days=-1),
    schedule=None,
    catchup=False,
    tags=['test', 'table']
) as dag:

    input_table = Table('dict_types', conn_id='source_db', metadata=Metadata(schema='public'))

    @aql.dataframe
    def modify_data(data: pd.DataFrame):
        data['some_column'] = 'new_value'
        return data

    @aql.dataframe
    def print_data(data: pd.DataFrame):
        print(data.info())
        return data

    data = load_table('table_data', 'source_db')
    modified_data = modify_data(data)
    print_data(modified_data)

