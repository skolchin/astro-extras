""" Sensors demo """

from airflow.models import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.datasets import Dataset
from astro_extras import days_ago, FileChangedSensor
from typing import Tuple

with DAG(
    dag_id='simple-sensor-demo',
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=['demo', 'sensors'],
) as dag:
    
    @dag.task(trigger_rule='none_skipped')
    def file_changed(event: Tuple[str, str]):
        if event:
            print(f'File {event[0]} changed at {event[1]}')
        else:
            print(f'File changed')

    @dag.task(trigger_rule='all_done')
    def nope():
        pass

    sensor = FileChangedSensor(
        task_id='file_sensor',
        filepath='test', 
        fs_conn_id='fs_test',
        deferrable=True,
        poke_interval=5.0,
        timeout=30.0,
        soft_fail=True,
    )

    file_changed(sensor.output) >> nope()

ds = Dataset('file_changed')

with DAG(
    dag_id='file-sensor-triggerer',
    start_date=days_ago(1),
    schedule='*/5 * * * *',
    catchup=False,
    tags=['demo', 'sensors'],
) as dag:
    
    FileChangedSensor(
        task_id='file_sensor',
        filepath='test', 
        fs_conn_id='fs_test',
        deferrable=True,
        poke_interval=5.0,
        timeout=5*30.0-5,
        soft_fail=True,
        outlets=[ds],
    )
    # FileSensor(
    #     task_id='file_sensor',
    #     filepath='test', 
    #     fs_conn_id='fs_test',
    #     deferrable=True,
    #     start_from_trigger=True,
    #     poke_interval=5.0,
    #     timeout=5*30.0-5,
    #     soft_fail=True,
    #     outlets=[ds],
    # )


with DAG(
    dag_id='file-sensor-processor',
    start_date=days_ago(1),
    schedule=[ds],
    catchup=False,
    tags=['demo', 'sensors'],
) as dag:
    
    @dag.task(inlets=[ds])
    def process_file(triggering_dataset_events=None):
        if not triggering_dataset_events:
            print('process_file() called with triggering_dataset_events==None')
        else:
            for dataset, dataset_list in triggering_dataset_events.items():
                print(dataset, dataset_list)
                print(dataset_list[0].source_dag_run.dag_id)

    process_file()
