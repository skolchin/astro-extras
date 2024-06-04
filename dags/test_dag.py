import pendulum
import pandas as pd
from airflow.models import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

# Declare Astro SDK tables
types_table = Table(name='types', conn_id='source_db', metadata=Metadata(schema='stage'))
types_copy_table = Table(name='types_copy', conn_id='source_db', metadata=Metadata(schema='stage'))

# Plain Airflow dataset
dataset = Dataset("myscheme://myhost?table=mytable")

@task
def print_triggering_dataset_events(triggering_dataset_events=None):
    """ Print out dataset trigger information """
    for dataset, event_list in triggering_dataset_events.items():
        print(f'Dataset: {dataset}')
        print(f'Events: {event_list}')

# First example

with DAG(
    dag_id='load_file',
    start_date=pendulum.today().add(days=-1),
    schedule='@daily',
    catchup=False,
    tags=['testing']
) as dag:
    """ Load file into TYPES table. This should modify `types_table` dataset and trigger corresponding DAG """
    aql.load_file(File(path='./dags/test.csv'), output_table=types_table)

with DAG(
    dag_id='triggered_by_file_load',
    start_date=pendulum.today().add(days=-1),
    schedule=[types_table],
    catchup=False,
    tags=['testing']
) as dag:
    """ This DAG is to be initiated by `types_table` dataset modifications """
    print_triggering_dataset_events()

# Second example

with DAG(
    dag_id='copy-table',
    start_date=pendulum.today().add(days=-1),
    schedule='@daily',
    catchup=False,
    tags=['testing']
) as dag:
    """ Load all data from TYPES table and save into new `TYPES_COPY` table. 
    This should modify `types_copy_table` dataset and trigger corresponding DAG """

    @aql.run_raw_sql(results_format='pandas_dataframe')
    def load_table(table: Table):
        return '''select * from {{table}}'''

    @aql.dataframe
    def save_data(data: pd.DataFrame):
        return data

    data = load_table(types_table)
    save_data(data, output_table=types_copy_table)

with DAG(
    dag_id='triggered_by_copy_table',
    start_date=pendulum.today().add(days=-1),
    schedule=[types_copy_table],
    catchup=False,
    tags=['testing']
) as dag:
    """ This DAG is to be initiated by `types_copy_table` dataset modifications """
    print_triggering_dataset_events()

# Third example

with DAG(
    dag_id='dataset_triggerer',
    start_date=pendulum.today().add(days=-1),
    schedule='@daily',
    catchup=False,
    tags=['testing']
) as dag:
    """ Simply trigger `dataset` dataset changes to run corresponding DAG  """

    @dag.task(outlets=[dataset])
    def trigger_dataset_event():
        print('Triggering event')

    trigger_dataset_event()

with DAG(
    dag_id='triggered_by_dataset',
    start_date=pendulum.today().add(days=-1),
    schedule=[dataset],
    catchup=False,
    tags=['testing']
) as dag:
    """ This DAG is to be initiated by `dataset` dataset modifications """
    print_triggering_dataset_events()

