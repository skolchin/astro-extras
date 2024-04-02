""" Sensors demo """

from airflow.models import DAG
from typing import Tuple
from astro_extras import days_ago, FileChangedSensor

with DAG(
    dag_id='sensor-demo',
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=['demo', 'sensors'],
) as dag:
    
    @dag.task(trigger_rule='none_skipped')
    def file_changed(event: Tuple[str, str]):
        print(f'File {event[0]} changed at {event[1]}')

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
