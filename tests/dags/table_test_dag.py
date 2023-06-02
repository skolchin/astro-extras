# Astro SDK Extras project
# (c) kol, 2023

import pendulum
import pandas as pd
from airflow.models import DAG
from astro import sql as aql
from astro.sql.table import Table, Metadata

from astro_extras import load_table, save_table

with DAG(
    dag_id='test-table-load_save',
    start_date=pendulum.today('Europe/Moscow').add(days=-1),
    schedule=None,
    catchup=False,
) as dag:

    input_table = Table('dict_types', conn_id='source_db', metadata=Metadata(schema='public'))
    output_table = Table('tmp_dict_types', conn_id='target_db', metadata=Metadata(schema='public'))

    @aql.dataframe
    def modify_data(data: pd.DataFrame):
        data['some_column'] = 'new_value'
        return data

    data = load_table(input_table)
    modified_data = modify_data(data)
    save_table(modified_data, output_table, fail_if_not_exist=False)

with DAG(
    dag_id='test-table-save_fail',
    start_date=pendulum.today('Europe/Moscow').add(days=-1),
    schedule=None,
    catchup=False,
) as dag:

    data = load_table('public.dict_types', 'source_db')
    save_table(data, 'public.tmp_dict_types2', conn_id='target_db')
