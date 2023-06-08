# Table

[Astro-sdk-extra Index](../README.md#astro-sdk-extra-index) /
[Operators](./index.md#operators) /
Table

> Auto-generated documentation for [operators.table](../../src/astro_extras/operators/table.py) module.

- [Table](#table)
  - [TableTransfer](#tabletransfer)
    - [TableTransfer().execute](#tabletransfer()execute)
  - [declare_tables](#declare_tables)
  - [load_table](#load_table)
  - [save_table](#save_table)
  - [transfer_table](#transfer_table)
  - [transfer_tables](#transfer_tables)

## TableTransfer

[Show source in table.py:26](../../src/astro_extras/operators/table.py#L26)

Customized table transfer operator. Usually is used within [transfer_table](#transfer_table) function

#### Signature

```python
class TableTransfer(GenericTransfer):
    def __init__(
        self,
        source_table: Table,
        destination_table: Table,
        mode: Optional[Literal["default", "delta", "full"]] = "default",
        session: Optional[ETLSession] = None,
        **kwargs
    ) -> None:
        ...
```

### TableTransfer().execute

[Show source in table.py:76](../../src/astro_extras/operators/table.py#L76)

#### Signature

```python
def execute(self, context: Context):
    ...
```



## declare_tables

[Show source in table.py:292](../../src/astro_extras/operators/table.py#L292)

Convert list of string table names to list of `Table` objects

#### Signature

```python
def declare_tables(
    table_names: Iterable[str], conn_id: Optional[str] = None
) -> List[Table]:
    ...
```



## load_table

[Show source in table.py:98](../../src/astro_extras/operators/table.py#L98)

Loads table into memory.

This is a wrapper over Astro-SDK `run_raw_sql` to
load data from given database table into XCom and make it available for
further processing.

SQL templating is supported, e.g. if a template for given table was found, it
will be executed to get the data (see `astro_extras.utils.template.get_template_file`).

Please note that in order to operate even on modest volumes of data,
intermediate XCom storage might be required. Easiest way to set it up is to use
local Parquet file storage by setting
`AIRFLOW__ASTRO_SDK__XCOM_STORAGE_CONN_ID=local` environment
variable. However, this will add extra serialization/deserialization
operation to every task thus increasing overall DAG execution time.

See https://astro-sdk-python.readthedocs.io/en/1.2.0/guides/xcom_backend.html
for details.

#### Arguments

- `table` - Either a table name or Astro-SDK `Table` object to load data from
- `conn_id` - Airflow connection ID to underlying database. If not specified,
    and `Table` object is passed it, its `conn_id` attribute will be used.
- `session` - `astro_extras.operators.session.ETLSession` object.
    Used only to link up to the `open_session` operator.
- `sql` - Custom SQL to load data, used only if no SQL template found.
    If neither SQL nor template is given, all table data will be loaded.

Results:
    `XComArg` object suitable for further manipulations with Astro-SDK functions

#### Examples

```python
>>> @aql.dataframe
>>> def modify_data(data: pd.DataFrame):
>>>     data['some_column'] = 'new_value'
>>>     return data
>>> data = load_table('test_table', conn_id='source_db')
>>> modified_data = modify_data(data)
>>> save_table(modified_data, conn_id='target_db')
```

#### Signature

```python
def load_table(
    table: Union[str, Table],
    conn_id: Optional[str] = None,
    session: Optional[ETLSession] = None,
    sql: Optional[str] = None,
) -> XComArg:
    ...
```



## save_table

[Show source in table.py:167](../../src/astro_extras/operators/table.py#L167)

Saves a table into database

#### Signature

```python
def save_table(
    data: XComArg,
    table: Union[str, Table],
    conn_id: Optional[str] = None,
    session: Optional[ETLSession] = None,
    fail_if_not_exist: Optional[bool] = True,
) -> XComArg:
    ...
```



## transfer_table

[Show source in table.py:195](../../src/astro_extras/operators/table.py#L195)

Cross-database data transfer.

This function implements cross-database geterogenous data transfer.

It reads data from source table into memory and then sequentaly inserts
each record into the target table. Fields order in the source and target tables
must be identical and field types must be compatible, or transfer will fail or produce
undesirable results.

To limit data selection or customize fields, a SQL template could be
created for the source table (see `astro_extras.utils.template.get_template_file`).
The template must ensure fields order and type compatibility with the target table.
If transfer is running under `astro_extras.operators.session.ETLSession` context,
a `session_id` field must also be manually added at proper place.

For example, if these tables are to participate in transfer:

create table source_data ( a int, b text );
create table target_data ( session_id int, b text, a int );

then, this SQL template might be created as `source_data.sql` file:

select {{ti.xcom_pull(key="session").session_id}} as session_id, b, a
from source_data;

#### Arguments

- `source` - Either a table name or a `Table` object which would be a data source.
    If a string name is provided, it may contain schema definition denoted by `.`.
    For `Table` objects, schema must be defined in `Metadata` field,
    otherwise Astro SDK might fall to use its default schema.
    If a SQL template exists for this table name, it will be executed,
    otherwise all table data will be selected.

- `target` - Either a table name or a `Table` object where data will be saved into.
    If a name is provided, it may contain schema definition denoted by `.`.
    For `Table` objects, schema must be defined in `Metadata` field,
    otherwise Astro SDK might fall to use its default schema.
    If omitted, `source` argument value is used (this makes sense only
    with string table name and different connections).

- `mode` - Reserved for furter use

- `source_conn_id` - Source database Airflow connection.
    Used only with string source table name; for `Table` objects, `conn_id` field is used.
    If omitted and `session` argument is provided, `session.source_conn_id` will be used.

- `destination_conn_id` - Destination database Airflow connection.
    Used only with string target table name; for `Table` objects, `conn_id` field is used.
    If omitted and `session` argument is provided, `session.destination_conn_id` will be used.

- `session` - `ETLSession` object. If set and no SQL template is defined,
    a `session_id` field will be automatically added to selection.

- `kwargs` - Any parameters passed to underlying [TableTransfer](#tabletransfer) operator (e.g. `preoperator`, ...)

#### Returns

`XComArg` object

#### Examples

Using `Table` objects (note use of `Metadata` object to specify schemas):

```python
>>> with DAG(...) as dag:
>>>     input_table = Table('table_data', conn_id='source_db',
>>>                          metadata=Metadata(schema='public'))
>>>     output_table = Table('table_data', conn_id='target_db',
>>>                          metadata=Metadata(schema='stage'))
>>>     transfer_table(input_table, output_table)
```

Using string table name:

```python
>>> with DAG(...) as dag, ETLSession('source_db', 'target_db') as sess:
>>>     transfer_table('public.table_data', session=sess)
```

#### Signature

```python
def transfer_table(
    source: Union[str, Table],
    target: Union[str, Table, None] = None,
    mode: Optional[Literal["default", "delta", "full"]] = "default",
    source_conn_id: Optional[str] = None,
    destination_conn_id: Optional[str] = None,
    session: Union[XComArg, ETLSession, None] = None,
    **kwargs
) -> XComArg:
    ...
```



## transfer_tables

[Show source in table.py:300](../../src/astro_extras/operators/table.py#L300)

Transfer multiple tables

#### Signature

```python
def transfer_tables(
    source_tables: List[Union[str, Table]],
    target_tables: Optional[List[Union[str, Table]]] = None,
    mode: Optional[Literal["default", "delta", "full"]] = "default",
    source_conn_id: Optional[str] = None,
    destination_conn_id: Optional[str] = None,
    group_id: Optional[str] = None,
    num_parallel: Optional[int] = 1,
    session: Union[XComArg, ETLSession, None] = None,
    **kwargs
) -> TaskGroup:
    ...
```