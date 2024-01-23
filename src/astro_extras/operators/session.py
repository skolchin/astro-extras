# Astro SDK Extras project
# (c) kol, 2023

""" ETL session management routines """

import re
from datetime import datetime, timedelta
from dateutil.parser import isoparse

from airflow.models import DAG, BaseOperator
from airflow.models.xcom_arg import XComArg
from airflow.models.dag import DagContext
from airflow.operators.python import get_current_context
from airflow.utils.context import Context
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.settings import TIMEZONE

from attr import define, field
from astro import sql as aql
from astro.sql.operators.raw_sql import RawSQLOperator
from typing import Optional, Union, Any, Tuple, cast

from ..utils.datetime_local import datetime_to_tz

@define(slots=False)
class ETLSession:
    """ Session data object. Holds all ETL session attributes, can be pushed to XCom.
    Implements context manager protocol (see examples). 
    
    See `open_session` function for more information.

    Args:
        source_conn_id:   Source connection ID
        destination_conn_id:   Destination connection ID
        session_conn_id:   ID of connection, where `public.sessions` table is located.
            If not set, then `destination_conn_id` is used
        session_id:   Actual session ID (automatically generated, do not set it manually)
        period_start: Date and time of session period start as ISO-format string.
            See `open_session` for details.
        period_end: Date and time of session period end as ISO-format string.
            See `open_session` for details.
        dag: DAG, where the session was created. Used only to pass DAG's reference throught.

    Examples:
        Using `ETLSession` as context manager:
        >>> with DAG(...) as dag, ETLSession('source', 'target') as session:
        >>>     transfer_table('test_table', session=session)

        Explicit call to `open_session`:        
        >>> with DAG(...) as dag:
        >>>     session = open_session('source', 'target')
        >>>     transfer_table('test_table', session=session)
        >>>     close_session(session)
    """
    source_conn_id: str = field(default=None)
    destination_conn_id: str = field(default=None)
    session_conn_id: str = field(default=None)
    session_id: int = field(default=0)
    period_start: str = field(default=None)
    period_end: str = field(default=None)
    dag: DAG = field(default=None)

    def __attrs_post_init__(self) -> None:
        if not self.session_conn_id:
            self.session_conn_id = self.destination_conn_id

    def __getstate__(self):
        return self.__dict__

    def serialize(self):
        return {
            'source_conn_id': self.source_conn_id,
            'destination_conn_id': self.destination_conn_id,
            'session_conn_id': self.session_conn_id,
            'session_id': self.session_id,
            'period_start': self.period_start,
            'period_end': self.period_end,
        }

    @staticmethod
    def deserialize(data, version: int):
        return ETLSession(
            source_conn_id=data['source_conn_id'],
            destination_conn_id=data['destination_conn_id'],
            session_conn_id=data['session_conn_id'],
            session_id=data['session_id'],
            period_start=data['period_start'],
            period_end=data['period_end'],
        )

    def __enter__(self):
        self._actual_sesssion = open_session(
            source_conn_id=self.source_conn_id,
            destination_conn_id=self.destination_conn_id,
            session_conn_id=self.session_conn_id,
            dag=self.dag)
        return self._actual_sesssion

    def __exit__(self, type, value, traceback):
        dag = cast(DAG, DagContext.get_current_dag())
        if len(dag.tasks) > 1:
            t1, t2 = dag.tasks[0], dag.tasks[1]
            if t1.task_id == 'open-session' and 'open-session' not in t2.upstream_task_ids:
                t2.set_upstream(t1)
        close_session(self._actual_sesssion).set_downstream(aql.cleanup())

class OpenSessionOperator(BaseOperator):
    """ Session opening operator. Normally is used within `open_session` function """

    def __init__(self,
                 *,
                 source_conn_id: Optional[str] = 'default',
                 destination_conn_id: Optional[str] = 'default',
                 session_conn_id: Optional[str] = None,
                 **kwargs):

        task_id = kwargs.pop('task_id', 'open-session')
        super().__init__(task_id=task_id, **kwargs)

        self.source_conn_id = source_conn_id
        self.destination_conn_id = destination_conn_id
        self.session_conn_id = session_conn_id if session_conn_id else destination_conn_id
        self.session: ETLSession = None

    def _new_session(self, period_start: str, period_end: str, context: Context) -> int:
        # TODO: use database-neutral statement with SQLA
        sql = f"""insert into public.sessions(source, target, period, started, status, run_id) 
            values('{self.source_conn_id}','{self.destination_conn_id}','{{ {period_start}, {period_end} }}', 
            '{datetime.now()}','running','{context['run_id']}') returning session_id"""

        op = RawSQLOperator(
            task_id=self.task_id,
            python_callable=lambda : sql,
            conn_id=self.session_conn_id,
            handler=lambda result: result.fetchone(),
            response_size=1)
        
        return op.execute(context)[0]

    def execute(self, context: Context):

        period_start, period_end = get_session_period(context)
        session_id = self._new_session(period_start, period_end, context)
        self.log.info(f'New session {session_id} for period [{period_start},{period_end}] started')

        session = ETLSession(
            source_conn_id=self.source_conn_id,
            destination_conn_id=self.destination_conn_id, 
            session_id=session_id, 
            session_conn_id=self.session_conn_id,
            period_start=period_start,
            period_end=period_end)
        
        context['ti'].xcom_push(key='session', value=session)
        return session

class CloseSessionOperator(BaseOperator):
    """ Session closing operator. Normally is used within `close_session` function """

    def __init__(self,
                 *,
                 session: Union[ETLSession, XComArg, None] = None,
                 **kwargs):

        task_id = kwargs.pop('task_id', 'close-session')
        trigger_rule = kwargs.pop('trigger_rule', TriggerRule.ALL_DONE)
        super().__init__(task_id=task_id, trigger_rule=trigger_rule, **kwargs)
        self.session = session

    def execute(self, context: Context):
        self.session = ensure_session(self.session)

        dag_run = context['dag_run']
        failed_tasks = [ti for ti in dag_run.get_task_instances(state=TaskInstanceState.FAILED)]
        status = 'error' if failed_tasks else 'success'

        # TBD: use of database-neutral statement
        sql = f"update public.sessions set finished=current_timestamp, status='{status}' " \
                   f"where session_id={self.session.session_id}"
        op = RawSQLOperator(
            task_id=self.task_id,
            python_callable=lambda : sql,
            conn_id=self.session.session_conn_id,
            response_size=1)
        op.execute(context)
        self.log.info(f'Session {self.session.session_id} closed with status {status}')

        if status == 'error':
            raise AirflowException('Setting drives to idle')

def open_session(
        source_conn_id: str, 
        destination_conn_id: str, 
        session_conn_id: Optional[str] = None, 
        dag: Optional[DAG] = None,
        **kwargs) -> XComArg:
    """ Opens a new ETL session.

    ETL session is a logical group of data transfers united by single identifier (`session_id`).
    Sessions store information about data transfer source, target, data loading period
    and completion state thus providing all necessary information about the data flow.

    Call to the `open_session` should be the 1st call in a data transfer DAG. It will
    create a new session by adding a record to `sessions` table and save a `ETLSession` instance 
    to XCom under "session" key. When using `ETLSession` class as a context manager,
    the call is performed implicitly.

    The session object could be retrieved from XCom and used within the queries.
        
    Note that ETL session object has `period_start` and `period_end` fields, which
    are calculated either automatically or could be specified manually by adding
    `{"period": "[<period_start>, <period_end>]"}` parameter when DAG is started. 
    These fields can be used in data extraction queries to limit dataset like this:

    ```sql
    select * from data_table 
    where some_date 
        between '{{ ti.xcom_pull(key="session").period_start }}'::timestamp
        and '{{ ti.xcom_pull(key="session").period_end }}'::timestamp
    ```

    Technically, sessions are stored in a `public.sessions` table. Table DDL (for Postgres):

    ```sql
    create table public.sessions(
        session_id serial not null primary key,
        source text not null,
        target text not null,
        period timestamptz[2] not null,
        run_id text,
        started timestamptz not null,
        finished timestamptz,
        status varchar(10) not null 
            check (status in ('running', 'success', 'error'))
    );
    ```

    Every table where the data is saved should have an extra `session_id` field 
    referencing the `sessions` table. For example:

        create table public.test_table(
            session_id int not null references public.sessions(session_id),
            id int not null,
            name text not null
        );
    
    This allows to easily identify when particular record was loaded or 
    clean up after unsuccessfull attempts.

    Args:
        source_conn_id:   Airflow connection where source data resides
        destination_conn_id:   Airflow connection to transfer data to
        session_conn_id:   ID of connection, where `sessions` table is located.
            If not set, then `destination_conn_id` is used.

    Returns:
        An `XComArg` placeholder object indicating the session was created 
        - due to Airflow's architecture actual session object could not be accessed 
        at this point, but it will automatically be converted to real one
        upon passing in to the TaskFlow's task function (see examples).

    Examples:
        Create a DAG which opens a session, outputs info to log and closes it:

        >>> with DAG(...) as dag, ETLSession('source', 'target') as session:
        >>>     @dag.task
        >>>     def print_session(session: ETLSession):
        >>>         print(session)
        >>>     print_session(session)

        Create a DAG with `open-session -> transfer-test_table -> close_session`
        task sequence:

        >>> with DAG(...) as dag:
        >>>     session = open_session('source', 'target')
        >>>     transfer_table('test_table', session=session)
        >>>     close_session(session)

        Connections named `source` and `target` must be defined pointing to corresponding
        databases. Table `public.sessions` must exists in the target database.

        Table `test_table` must have the same structure in both databases, except
        that `session_id` field must be added to the target table as 1st column.

        See `astro_extras.operators.table.transfer_table` function for details on transfer operation.
    """
    
    assert (dag := dag or DagContext.get_current_dag())
    return XComArg(OpenSessionOperator(
        dag=dag,
        source_conn_id=source_conn_id,
        destination_conn_id=destination_conn_id,
        session_conn_id=session_conn_id,
        **kwargs))

def close_session(
        session: Union[ETLSession, XComArg], 
        upstream_task: Optional[Any] = None,
        dag: Optional[DAG] = None,
        **kwargs) -> XComArg:
    """ Closes the ETL session.

    Updates `public.sessions` table for currently running session
    saving completion time and state. If all tasks in a DAG run were successfull,
    session's state will be set to `success`, otherwise - to `error`. Note that 
    session closing task will also fail in later case in order to be
    automatically included into further retrying attempts.
    
    This function should be the last call in a DAG, and it will automatically try to link up 
    to the end of the task chain.
    
    If a `ETLSession` class is used as a context manager, `close_session` will be called
    implicitly.

    Args:
        session:    An object returned by `open_session`
        upstream_task:  A DAGs task which `close_session` task
            would be linked to. If not set, a last DAG's task will be used.

    Returns:
        An `XComArg` object indicating the session was closed

    Examples:
        See `open_session` for examples
    """
    
    assert (dag := dag or DagContext.get_current_dag())
    if upstream_task is None:
        if not len(dag.tasks):
            raise ValueError('close_session must not be the first DAG operator')
        upstream_task = dag.tasks[-1]

    op = CloseSessionOperator(dag=dag, session=session, **kwargs)
    upstream_task.set_downstream(op)
    return XComArg(op)

def get_current_session(context: Optional[Context] = None) -> ETLSession:
    """ Retrieves current session from XCom.
    
    Args:
        context:    DAG execution context (optional)

    Returns:
        Current ETL session as `ETLSession` class instance
    """
    context = context or get_current_context()
    return context['ti'].xcom_pull(key='session')

def ensure_session(session: Optional[Union[ETLSession, XComArg]], 
                   context: Optional[Context] = None) -> ETLSession:
    """ Returns current session. If a placeholder object returned by `open_session` is passed in,
    retrieves actual session from XCom. 
    
    Args:
        session: Either the `ETLSession` instance or a placeholder returned by `open_session` call
        context:    DAG execution context (optional)

    Returns:
        Current ETL session as `ETLSession` class instance
    """
    if session is None:
        return None
    if isinstance(session, ETLSession):
        return session
    if isinstance(session, XComArg):
        return get_current_session(context)
    raise TypeError(f'Either ETLSession or XComArg expected, {session.__class__.__name__} found')

_TS_REGX = r'\d{4}-([0]\d|1[0-2])-([0-2]\d|3[01])(T\d{2}:\d{2}:\d{2})?'
_FULL_REGX = r'\[\d{4}-([0]\d|1[0-2])-([0-2]\d|3[01])(T\d{2}:\d{2}:\d{2})?,\s*\d{4}-([0]\d|1[0-2])-([0-2]\d|3[01])(T\d{2}:\d{2}:\d{2})?]'

def get_session_period(context: Optional[Context] = None) -> Tuple[str, str]:
    """ Calculates ETL session loading period.
    
    This function is used when a new session is created. It recognizes a "period"
    DAG run parameter, which must be provided as two valid dates or datetimes defining
    lower and upper bound of loading period. If a date-only upper bound is used,
    it will be increased to hold entire day (see examples).
    
    If this option was not specified, loading period will be set as interval of
    `[data_interval_start, data_interval_end]` Airflow variables 
    (see https://docs.astronomer.io/learn/scheduling-in-airflow for details).
    However, these dates will be converted to Airflow default timezone 
    (as they are defined in UTC).

    Args:
        context:    DAG execution context (optional)

    Returns:
        Tuple of two datetimes indicating lower- and upper-bound of loading period,
        converted to ISO-formatted strings (see `datetime.isoformat()`).

    Examples:
        Examples of DAG run configuration options and their conversion:

        ```
        {"period": "[2023-05-01, 2023-05-31]"}
            -> ["2023-05-01T00:00:00", "2023-06-01T00:00:00"]
        {"period": "[2023-05-01, 2023-05-01]"} 
            -> ["2023-05-01T00:00:00", "2023-05-02T00:00:00"]
        {"period": "[2023-05-01T12:00:00, 2023-05-01T14:00:00]"} 
            -> ["2023-05-01T10:00:00", "2023-05-01T14:00:00"]
        ```

    """
    context = context or get_current_context()

    if (period_str := context['dag_run'].conf.get('period')):
        if not re.match(_FULL_REGX, period_str):
            raise AirflowFailException('Period must be specified as "period": "[<date_from>,<date_to>]"')
        
        period = [isoparse(x.group(0)) for x in re.finditer(_TS_REGX, period_str)]
        if len(period) < 2:
            raise AirflowFailException('Invalid period: two valid dates in YYYY-MM-DD format must be specified')

        # If no time part is provided in the upper period bound,
        # consider this to be date-only and add 1 day to align to days'end
        # For example, specifying period = ["2023-05-01", "2023-05-01"] will be converted
        # to ["2023-05-01 00:00:00", "2023-05-02 00:00:00"]
        if period[1].hour == 0 and period[1].minute == 0 and period[1].second == 0:
            period[1] += timedelta(days=1)

        if period[1] <= period[0]:
            raise AirflowFailException('Upper period bound must be greater than lower bound')
    else:
        period = [context['data_interval_start'], context['data_interval_end']]
        period = [datetime_to_tz(x, TIMEZONE) for x in period]

    return tuple([x.isoformat() for x in period])
