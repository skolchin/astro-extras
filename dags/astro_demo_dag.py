# Astro SDK Extras project
# (c) kol, 2023

""" DAGs for Astro SDK caps demo """

import pendulum
import pandas as pd
from airflow.models import DAG
from astro import sql as aql
from astro.sql.table import Table, Metadata

with DAG(
    dag_id='astro-demo',
    start_date=pendulum.today().add(days=-1),
    schedule='@daily',
    catchup=False,
    tags=['demo', 'astro'],
) as dag:

    input_table = Table('types', conn_id='source_db', metadata=Metadata(schema='public'))
    output_table = Table('types_copy', conn_id='source_db', metadata=Metadata(schema='public'))

    @aql.run_raw_sql(handler=lambda result: result.fetchall(), results_format='pandas_dataframe')
    def load_table(table: Table):
        return '''select * from {{table}}'''

    @aql.dataframe
    def modify_data(data: pd.DataFrame):
        data['some_column'] = 'new_value'
        return data

    data = load_table(input_table)
    modify_data(data, output_table=output_table)
