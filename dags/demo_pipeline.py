# Astro SDK Extras project
# (c) kol, 2023

""" Demo ETL pipeline DAGs """

import pendulum
from airflow.models import DAG
from astro.sql.table import Table, Metadata
from astro_extras import transfer_table, ETLSession, run_sql_template

# Forward table declaration
source_types_table = Table('types', 'source_db', metadata=Metadata(schema='public'))
source_data_table = Table('table_data', 'source_db', metadata=Metadata(schema='public'))
stage_types_table = Table('types', 'target_db', metadata=Metadata(schema='stage'))
stage_data_table = Table('table_data', 'target_db', metadata=Metadata(schema='stage'))
dwh_dim_types_table = Table('dim_types', 'target_db', metadata=Metadata(schema='dwh'))
dwh_data_fact_table = Table('data_facts', 'target_db', metadata=Metadata(schema='dwh'))

with DAG(
    dag_id='source-stage-load',
    start_date=pendulum.today().add(days=-1),
    schedule='@daily',
    catchup=False,
    tags=['demo', 'pipeline'],
) as dag, ETLSession('source_db', 'target_db') as session:
    """ Source-to-stage data upload DAG """

    # This will upload data from source database into STAGE area in target database.
    # STAGE area reflects the source database except that it elmiminates
    # all primary keys and unique constraints in order to keep multiple
    # versions of every record.
    #
    # TYPES table is transferred in `dict` mode that is data uploaded only if changed
    #
    # For TABLE_DATA, SQL template is used to select only relevant data 
    # (for demo purpose it simply selects all)

    transfer_table(source_types_table, stage_types_table, mode='dict', session=session) >> \
    transfer_table(source_data_table, stage_data_table, session=session)

with DAG(
    dag_id='stage-dwh-load',
    start_date=pendulum.today().add(days=-1),
    schedule=[stage_types_table, stage_data_table],
    catchup=False,
    tags=['demo', 'pipeline'],
) as dag, ETLSession('target_db', 'target_db') as session:
    """ DWH data processing DAG """

    # This will process data stored in STAGE and updates DWH data
    run_sql_template('merge_dim_types', 'target_db',
                     input_tables=[stage_types_table],
                     affected_tables=[dwh_dim_types_table]) >> \
    run_sql_template('update_data_fact', 'target_db',
                     input_tables=[stage_data_table],
                     affected_tables=[dwh_data_fact_table]) 
