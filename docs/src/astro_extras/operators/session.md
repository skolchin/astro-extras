# Session

[astro_extras Index](../../../README.md#astro_extras-index) /
`src` /
[Astro Extras](../index.md#astro-extras) /
[Operators](./index.md#operators) /
Session

> Auto-generated documentation for [src.astro_extras.operators.session](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/session.py) module.

- [Session](#session)
  - [CloseSessionOperator](#closesessionoperator)
    - [CloseSessionOperator().execute](#closesessionoperator()execute)
  - [ETLSession](#etlsession)
    - [ETLSession.deserialize](#etlsessiondeserialize)
    - [ETLSession().serialize](#etlsession()serialize)
  - [OpenSessionOperator](#opensessionoperator)
    - [OpenSessionOperator().execute](#opensessionoperator()execute)
  - [close_session](#close_session)
  - [ensure_session](#ensure_session)
  - [get_current_session](#get_current_session)
  - [get_session_period](#get_session_period)
  - [open_session](#open_session)

## CloseSessionOperator

[Show source in session.py:156](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/session.py#L156)

Session closing operator. Normally is used within [close_session](#close_session) function

#### Signature

```python
class CloseSessionOperator(BaseOperator):
    def __init__(self, session: Union[ETLSession, XComArg, None] = None, **kwargs):
        ...
```

#### See also

- [ETLSession](#etlsession)

### CloseSessionOperator().execute

[Show source in session.py:169](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/session.py#L169)

#### Signature

```python
def execute(self, context: Context):
    ...
```



## ETLSession

[Show source in session.py:28](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/session.py#L28)

 Session data object. Holds all ETL session attributes, can be pushed to XCom.
Implements context manager protocol (see examples).

#### Arguments

- `source_conn_id` - Source connection ID
- `destination_conn_id` - Destination connection ID
- `session_conn_id` - ID of connection, where `public.sessions` table is located.
    If not set, then `destination_conn_id` is used
- `session_id` - Actual session ID (automatically generated, do not set it)
- `period_start` - Date and time of session period start as ISO-format string.
    See [open_session](#open_session) for details.
- `period_end` - Date and time of session period end as ISO-format string.
    See [open_session](#open_session) for details.
- `dag` - DAG, where the session was created. Used only to pass DAG's reference throught.

#### Examples

Using [ETLSession](#etlsession) as context manager:

```python
>>> with DAG(...) as dag, ETLSession('source', 'target') as session:
>>>     transfer_table('test_table', session=session)
```

Explicit call to [open_session](#open_session):

```python
>>> with DAG(...) as dag:
>>>     session = open_session('source', 'target')
>>>     transfer_table('test_table', session=session)
>>>     close_session(session)
```

#### Signature

```python
class ETLSession:
    ...
```

### ETLSession.deserialize

[Show source in session.py:80](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/session.py#L80)

#### Signature

```python
@staticmethod
def deserialize(data, version: int):
    ...
```

### ETLSession().serialize

[Show source in session.py:70](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/session.py#L70)

#### Signature

```python
def serialize(self):
    ...
```



## OpenSessionOperator

[Show source in session.py:107](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/session.py#L107)

Session opening operator. Normally is used within [open_session](#open_session) function

#### Signature

```python
class OpenSessionOperator(BaseOperator):
    def __init__(
        self,
        source_conn_id: Optional[str] = "default",
        destination_conn_id: Optional[str] = "default",
        session_conn_id: Optional[str] = None,
        **kwargs
    ):
        ...
```

### OpenSessionOperator().execute

[Show source in session.py:140](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/session.py#L140)

#### Signature

```python
def execute(self, context: Context):
    ...
```



## close_session

[Show source in session.py:291](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/session.py#L291)

Closes the ETL session.

Updates `public.sessions` table for currently running session
saving completion time and state. If all tasks in a DAG run were successfull,
session's state will be set to `success`, otherwise - to `error`. Note that
session closing task will also fail in later case in order to be
automatically included into further retrying attempts.

This function should be the last call in a DAG, and it will automatically try to link up
to the end of the task chain.

If a [ETLSession](#etlsession) class is used as a context manager, [close_session](#close_session) will be called
implicitly.

#### Arguments

- `session` - An object returned by [open_session](#open_session)
- `upstream_task` - A DAGs task which [close_session](#close_session) task
    would be linked to. If not set, a last DAG's task will be used.

#### Returns

An `XComArg` object indicating the session was closed

#### Examples

See [open_session](#open_session) for examples

#### Signature

```python
def close_session(
    session: Union[ETLSession, XComArg],
    upstream_task: Optional[Any] = None,
    dag: Optional[DAG] = None,
    **kwargs
) -> XComArg:
    ...
```

#### See also

- [ETLSession](#etlsession)



## ensure_session

[Show source in session.py:344](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/session.py#L344)

 Returns current session. If a placeholder object returned by [open_session](#open_session) is passed in,
retrieves actual session from XCom.

#### Arguments

- `session` - Either the [ETLSession](#etlsession) instance or a placeholder returned by [open_session](#open_session) call
- `context` - DAG execution context (optional)

#### Returns

Current ETL session as [ETLSession](#etlsession) class instance

#### Signature

```python
def ensure_session(
    session: Optional[Union[ETLSession, XComArg]], context: Optional[Context] = None
) -> ETLSession:
    ...
```

#### See also

- [ETLSession](#etlsession)



## get_current_session

[Show source in session.py:332](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/session.py#L332)

Retrieves current session from XCom.

#### Arguments

- `context` - DAG execution context (optional)

#### Returns

Current ETL session as [ETLSession](#etlsession) class instance

#### Signature

```python
def get_current_session(context: Optional[Context] = None) -> ETLSession:
    ...
```

#### See also

- [ETLSession](#etlsession)



## get_session_period

[Show source in session.py:367](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/session.py#L367)

Calculates ETL session loading period.

This function is used when a new session is created. It recognizes a "period"
DAG run parameter, which must be provided as two valid dates or datetimes defining
lower and upper bound of loading period. If a date-only upper bound is used,
it will be increased to hold entire day (see examples).

If this option was not specified, loading period will be set as interval of
`[data_interval_start, data_interval_end]` Airflow variables
(see https://docs.astronomer.io/learn/scheduling-in-airflow for details).
However, these dates will be converted to Airflow default timezone
(as they are defined in UTC).

#### Arguments

- `context` - DAG execution context (optional)

#### Returns

Tuple of two datetimes indicating lower- and upper-bound of loading period,
converted to ISO-formatted strings (see `datetime.isoformat()`).

#### Examples

Examples of DAG run configuration options and their conversion:

```
{"period": "[2023-05-01, 2023-05-31]"}
    -> ["2023-05-01T00:00:00", "2023-06-01T00:00:00"]
{"period": "[2023-05-01, 2023-05-01]"}
    -> ["2023-05-01T00:00:00", "2023-05-02T00:00:00"]
{"period": "[2023-05-01T12:00:00, 2023-05-01T14:00:00]"}
    -> ["2023-05-01T10:00:00", "2023-05-01T14:00:00"]
```

#### Signature

```python
def get_session_period(context: Optional[Context] = None) -> Tuple[str, str]:
    ...
```



## open_session

[Show source in session.py:190](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/session.py#L190)

Opens a new ETL session.

ETL session is a logical group of data transfers united by single identifier (`session_id`).
Sessions store information about data transfer source, target, data loading period
and completion state thus providing all necessary information about the data flow.

Call to the [open_session](#open_session) should be the 1st call in a data transfer DAG. It will
create a new session by adding a record to `sessions` table and save a [ETLSession](#etlsession) instance
to XCom under "session" key. When using [ETLSession](#etlsession) class as a context manager,
the call is performed implicitly.

The session object could be retrieved from XCom and used within the queries.

Note that ETL session object has `period_start` and `period_end` fields, which
are calculated either automatically or could be specified manually by adding
`{"period": "[<period_start>, <period_end>]"}` parameter when DAG is started.
These fields can be used in data extraction queries to limit dataset like this:

select * from data_table
where some_date
    between '{{ ti.xcom_pull(key="session").period_start }}'::timestamp
    and '{{ ti.xcom_pull(key="session").period_end }}'::timestamp

Technically, sessions are stored in a `public.sessions` table. Table DDL (for Postgres):

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

Every table where the data is saved should have an extra `session_id` field
referencing the `sessions` table. For example:

create table public.test_table(
    session_id int not null references public.sessions(session_id),
    id int not null,
    name text not null
);

This allows to easily identify when particular record was loaded or
clean up after unsuccessfull attempts.

#### Arguments

- `source_conn_id` - Airflow connection where source data resides
- `destination_conn_id` - Airflow connection to transfer data to
- `session_conn_id` - ID of connection, where `sessions` table is located.
    If not set, then `destination_conn_id` is used.

#### Returns

An `XComArg` placeholder object indicating the session was created
- due to Airflow's architecture actual session object could not be accessed
at this point, but it will automatically be converted to real one
upon passing in to the TaskFlow's task function (see examples).

#### Examples

Create a DAG which opens a session, outputs info to log and closes it:

```python
>>> with DAG(...) as dag, ETLSession('source', 'target') as session:
>>>     @dag.task
>>>     def print_session(session: ETLSession):
>>>         print(session)
>>>     print_session(session)
```

Create a DAG with `open-session -> transfer-test_table -> close_session`
task sequence:

```python
>>> with DAG(...) as dag:
>>>     session = open_session('source', 'target')
>>>     transfer_table('test_table', session=session)
>>>     close_session(session)
```

Connections named `source` and `target` must be defined pointing to corresponding
databases. Table `public.sessions` must exists in the target database.

Table `test_table` must have the same structure in both databases, except
that `session_id` field must be added to the target table as 1st column.

See `astro_extras.operators.table.transfer_table` function for details on transfer operation.

#### Signature

```python
def open_session(
    source_conn_id: str,
    destination_conn_id: str,
    session_conn_id: Optional[str] = None,
    dag: Optional[DAG] = None,
    **kwargs
) -> XComArg:
    ...
```