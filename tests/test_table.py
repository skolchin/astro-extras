# Astro SDK Extras project
# (c) kol, 2023

""" Table unit tests """

import pandas as pd
from test_utils import run_dag, logger

def test_table_load_save(docker_ip, docker_services, airflow_credentials, target_db):
    logger.info(f'Testing table load and save')
    result = run_dag('test-table-load_save', docker_ip, docker_services, airflow_credentials)
    assert result == 'success'

    with target_db.begin() as conn:
        data = pd.read_sql_table('tmp_types', conn)
        assert data.shape[0] == 3
        assert 'some_column' in data.columns

def test_table_save_fail(docker_ip, docker_services, airflow_credentials):
    logger.info(f'Testing table save to non-existing table')
    result = run_dag('test-table-save_fail', docker_ip, docker_services, airflow_credentials)
    assert result == 'failed'
