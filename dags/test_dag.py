# Astro SDK Extras project
# (c) kol, 2023

import pendulum
import pandas as pd
from airflow.models import DAG
from astro import sql as aql
from astro.sql.table import Table, Metadata

with DAG(
    dag_id='test',
    start_date=pendulum.today('Europe/Moscow').add(days=-1),
    schedule='@daily',
    catchup=False,
) as dag:

    input_table = Table('dict_types', conn_id='source_db', metadata=Metadata(schema='public'))

    @aql.run_raw_sql(handler=lambda result: result.fetchall(),
                    results_format='pandas_dataframe')
    def load_table(table: Table):
        return 'select * from {{table}}'

    @aql.dataframe(if_exists='fail')
    def table(data: pd.DataFrame):
        return data

    df = load_table(input_table)
