# Astro SDK Extras project
# (c) kol, 2023

""" Demo ETL pipeline DAGs """

import pendulum
from airflow.models import DAG
from astro.sql.table import Table, Metadata
from astro_extras import *

# Forward table declaration
source_tables = declare_tables([
    'types',
    'table_data',
    'ods_data'], schema='public', conn_id='source_db')
stage_tables = declare_tables([
    'types',
    'table_data',
    'ods_data'], schema='stage', conn_id='stage_db')
actuals_tables = declare_tables([
    'types',
    'table_data',
    'ods_data'], schema='actuals', conn_id='stage_db')
dwh_tables = declare_tables([
    'dim_types',
    'data_facts',
    'ods_data'], schema='dwh', conn_id='dwh_db')
fake_stage_tables = declare_tables([
    'types_x',
    'table_data_x',
    'ods_data_x'], schema='stage', conn_id='stage_db')

with DAG(
    dag_id='source-stage-load',
    start_date=pendulum.today().add(days=-1),
    schedule='@daily',
    catchup=False,
    tags=['demo', 'pipeline'],
) as dag, ETLSession('source_db', 'stage_db', 'stage_db') as session:
    """ Source-to-stage data upload DAG """

    # This will upload data from source database into STAGE area in target database.
    # STAGE area reflects the source database except that it elmiminates
    # all primary keys and unique constraints in order to keep multiple
    # versions of every record.
    #
    # For TYPES table only changed records are transferred
    #
    # For TABLE_DATA, SQL template is used to select only relevant data 
    # (for demo purpose it simply selects all)

    # transfer_changed_table(source_tables[0], stage_tables[0], session=session) >> \
    # transfer_table(source_tables[1], stage_tables[1], session=session) >> \
    # transfer_ods_table(source_tables[2], stage_tables[2], session=session)

    transfer_stage(
        [source_tables[0]], 
        [stage_tables[0]], 
        session=session,
        mode='compare') >> \
    transfer_stage(
        [source_tables[1]], 
        [stage_tables[1]], 
        session=session,
        mode='normal') >> \
    transfer_stage(
        [source_tables[2]], 
        [stage_tables[2]], 
        session=session,
        mode='sync')

with DAG(
    dag_id='stage-actuals-load',
    start_date=pendulum.today().add(days=-1),
    schedule=stage_tables,
    catchup=False,
    tags=['demo', 'pipeline'],
) as dag, ETLSession('stage_db', 'stage_db', 'stage_db') as session:
    """ Stage-to-actuals data upload DAG """

    # transfer_actuals_tables(
    #     fake_stage_tables[:-1], 
    #     actuals_tables[:-1], 
    #     session=session,
    #     keep_temp_table=False) >> \
    # transfer_actuals_table(
    #     fake_stage_tables[-1], 
    #     actuals_tables[-1], 
    #     session=session, 
    #     as_ods=True,
    #     keep_temp_table=False)

    transfer_actuals(
        stage_tables[:-1], 
        actuals_tables[:-1], 
        session=session,
        mode='update') >> \
    transfer_actuals(
        fake_stage_tables[-1], 
        actuals_tables[-1], 
        session=session, 
        mode='sync')

with DAG(
    dag_id='actuals-dwh-load',
    start_date=pendulum.today().add(days=-1),
    schedule=actuals_tables,
    catchup=False,
    tags=['demo', 'astro-extras'],
) as dag, ETLSession('actuals_db', 'dwh_db', 'stage_db') as session:
    """ actuals-to-dwh data processing DAG """

    # Update TYPES timed dictionary
    # Update DATA_FACT dwh table
    # update_timed_table(actuals_types_table, dwh_dim_types_table, session=session) >> \
    # run_sql_template('update_data_fact', 'dwh_db',
    #                  input_tables=[actuals_data_table, dwh_dim_types_table],
    #                  affected_tables=[dwh_data_fact_table]) 
    @dag.task
    def print_info(**context):
        print(f'Running with {context}')