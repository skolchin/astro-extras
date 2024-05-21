# Astro SDK Extras project
# (c) kol, 2023

""" Demo ETL pipeline DAGs """

import pendulum
from airflow.models import DAG
from astro.sql.table import Table, Metadata
from astro_extras import *

# Forward table declaration
source_types_table = Table('TYPES', 'source_db')
source_data_table = Table('table_data', 'source_db', metadata=Metadata(schema='public'))
source_ods_table = Table('ods_data', 'source_db')
stage_types_table = Table('types', 'stage_db', metadata=Metadata(schema='stage'))
stage_data_table = Table('TABLE_DATA', 'stage_db', metadata=Metadata(schema='stage'))
stage_ods_table = Table('ods_data', 'stage_db', metadata=Metadata(schema='STAGE'))
actuals_types_table = Table('types', 'stage_db', metadata=Metadata(schema='actuals'))
actuals_data_table = Table('table_data', 'stage_db', metadata=Metadata(schema='actuals'))
actuals_ods_table = Table('ODS_DATA', 'stage_db', metadata=Metadata(schema='actuals'))
dwh_dim_types_table = Table('dim_types', 'dwh_db', metadata=Metadata(schema='dwh'))
dwh_data_fact_table = Table('data_facts', 'dwh_db', metadata=Metadata(schema='dwh'))

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

    transfer_changed_table(source_types_table, stage_types_table, session=session) >> \
    transfer_table(source_data_table, stage_data_table, session=session) >> \
    transfer_ods_table(source_ods_table, stage_ods_table, session=session)

with DAG(
    dag_id='stage-actuals-load',
    start_date=pendulum.today().add(days=-1),
    schedule=[stage_types_table, stage_data_table, stage_ods_table],
    catchup=False,
    tags=['demo', 'pipeline'],
) as dag, ETLSession('stage_db', 'stage_db', 'stage_db') as session:
    """ Stage-to-actuals data upload DAG """

    transfer_actuals_tables(
        [stage_types_table, stage_data_table], 
        [actuals_types_table, actuals_data_table], 
        session=session,
        keep_temp_table=True) >> \
    transfer_actuals_table(
        stage_ods_table, 
        actuals_ods_table, 
        session=session, 
        as_ods=True,
        keep_temp_table=False)

with DAG(
    dag_id='actuals-dwh-load',
    start_date=pendulum.today().add(days=-1),
    schedule=[actuals_types_table, actuals_data_table],
    catchup=False,
    tags=['demo', 'astro-extras'],
) as dag, ETLSession('actuals_db', 'dwh_db', 'stage_db') as session:
    """ actuals-to-dwh data processing DAG """

    # Update TYPES timed dictionary
    # Update DATA_FACT dwh table
    update_timed_table(actuals_types_table, dwh_dim_types_table, session=session) >> \
    run_sql_template('update_data_fact', 'dwh_db',
                     input_tables=[actuals_data_table, dwh_dim_types_table],
                     affected_tables=[dwh_data_fact_table]) 
