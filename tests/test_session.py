# Astro SDK Extras project
# (c) kol, 2023

""" ETL session unit tests """

from dateutil.parser import isoparse
from confsupport import run_dag, logger
from sqlalchemy import text

def get_last_session(target_db):
    sql = 'select * from sessions where session_id = (select max(session_id) from sessions)'
    with target_db.begin() as conn:
        cursor = conn.execute(text(sql))
        return cursor.mappings().one_or_none()

def get_last_session_id(target_db):
    last_session = get_last_session(target_db)
    return last_session['session_id'] if last_session else 0

def compare_period(session_period, expected_period):
    p = tuple([t.replace(tzinfo=None) for t in session_period])
    x = tuple([isoparse(t) for t in expected_period])
    return p == x

def test_session_simple(docker_ip, docker_services, airflow_credentials, target_db):
    logger.info(f'Testing session opening')
    prev_session_id = get_last_session_id(target_db)
    result = run_dag('test-session', docker_ip, docker_services, airflow_credentials)
    assert result == 'success'
    new_session = get_last_session(target_db)
    assert new_session
    logger.info(f'Latest session: {new_session}')
    assert new_session['session_id'] > prev_session_id
    assert new_session['status'] == 'success'

def test_session_ctx(docker_ip, docker_services, airflow_credentials, target_db):
    logger.info(f'Testing session opening using context manager semantics')
    prev_session_id = get_last_session_id(target_db)
    result = run_dag('test-session-ctx', docker_ip, docker_services, airflow_credentials)
    assert result == 'success'
    new_session = get_last_session(target_db)
    assert new_session
    logger.info(f'Latest session: {new_session}')
    assert new_session['session_id'] > prev_session_id
    assert new_session['status'] == 'success'

def test_session_period(airflow, docker_ip, docker_services, airflow_credentials, target_db):
    logger.info(f'Testing manual period assignment')
    result = run_dag('test-session', docker_ip, docker_services, airflow_credentials,
                     params='{"period": "[2023-05-01, 2023-05-31]"}')
    assert result == 'success'
    new_session = get_last_session(target_db)
    assert new_session
    logger.info(f'Latest session: {new_session}')
    assert new_session['status'] == 'success'
    assert compare_period(new_session['period'], ['2023-05-01T00:00:00', '2023-05-31T00:00:00'])

def test_session_timestamp_period(airflow, docker_ip, docker_services, airflow_credentials, target_db):
    logger.info(f'Testing manual timestamp period assignment')
    result = run_dag('test-session', docker_ip, docker_services, airflow_credentials,
                     params='{"period": "[2023-05-01T12:00:00, 2023-05-01T14:00:00]"}')
    assert result == 'success'
    new_session = get_last_session(target_db)
    assert new_session
    logger.info(f'Latest session: {new_session}')
    assert new_session['status'] == 'success'
    assert compare_period(new_session['period'], ['2023-05-01T12:00:00', '2023-05-01T14:00:00'])

def test_session_wrong_period(airflow, docker_ip, docker_services, airflow_credentials, target_db):
    logger.info(f'Testing invalid periods assignment')
    for p in '[2023-05-02, 2023-05-01]', '[2023-05-02]', 'aaa':
        result = run_dag('test-session', docker_ip, docker_services, airflow_credentials,
                        params='{"period": "' + p + '"}')
        assert result == 'failed'
