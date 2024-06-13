# Astro SDK Extras project
# (c) kol, 2023

""" Templates unit tests """

import pandas as pd
from confsupport import run_dag, logger

def test_run_template_list(docker_ip, docker_services, airflow_credentials, source_db):
    logger.info(f'Testing template execution')
    result = run_dag('test-run-template-list', docker_ip, docker_services, airflow_credentials)
    assert result == 'success'

    with source_db.begin() as conn:
        orig_data = pd.read_sql_table('test_table_1', conn)
        new_data = pd.read_sql_table('tmp_table_data_1', conn)
        assert new_data.shape == orig_data.shape
        new_data = pd.read_sql_table('tmp_table_data_2', conn)
        assert new_data.shape == orig_data.shape

def test_run_template_session(docker_ip, docker_services, airflow_credentials, source_db):
    logger.info(f'Testing template execution in session context')
    result = run_dag('test-run-template-session', docker_ip, docker_services, airflow_credentials)
    assert result == 'success'

    with source_db.begin() as conn:
        orig_data = pd.read_sql_table('test_table_1', conn)
        new_data = pd.read_sql_table('tmp_table_data_3', conn)
        assert new_data.shape[0] == orig_data.shape[0]
        assert 'session_id' in new_data.columns
