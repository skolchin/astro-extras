# Astro SDK extras project

We've been using [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/)
as ETL tool for quite some time. Recently I came across
[Astronomer's Astro SDK](https://docs.astronomer.io/astro)
which simplifies data processig with Airflow very much by providing
functionality to easily load or save data,
define custom transformations, adding transparent support of newest
[Dataset](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html)
concept, automatic lineage and so on.

The goal of this project is to add some usefull features to these great products, such as:

* ETL session concept
* SQL file-based templates support
* Simplified table processing routines
* Heterogenios data transfer
* and more

Please note that this project is in the very early stage of development
so anything could change in the future.

## Concepts

### ETL Session

ETL session is a logical group of data transfers united by single identifier (`session_id`).
Sessions store information about data transfer source, target, data loading period
and completion state thus providing all necessary information about the data flow.

ETL session object defines its own data loading interval and allows
to override automatic interval boundaries calculation using parameters when a DAG is started.

Sessions are stored in a database in `sessions` table. Entries to that table are created
when a new session is instantiated withing a DAG run and closed when DAG
is finished.

ETL session instance is pushed to XCom under `session` key 
and becomes available to all operators within the data pipeline (see query example below).

### SQL Templates

SQL template is a file, which contains instructions and Jinja-macros substituted at runtime. 
For example, this template could be created in order to load data from `data_table` table 
within the limited date range:

``` sql
select * from data_table
where some_date between '{{ ti.xcom_pull(key="session").period_start }}'::timestamp
                and '{{ ti.xcom_pull(key="session").period_end }}'::timestamp
```

In order to be used, template file must be named after target object name
and placed in a `templates\<dag_id>` folder under DAGS_ROOT. In given case, 
assuming it will be used in `test-data-load` DAG,
template file name must be `./dags/templates/data_table.sql`.

Note that it uses an `ETLSession` object from XCom to filter only actual data.

### Heterogenuos data transfer

Original Astro-SDK misses functionality to transfer data between different
database systems (for example, from Oracle to PostgreSQL or vise versa).

However, there is a [GenericTransfer](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/generic_transfer/index.html) 
operator available in original Airflow code, so this package adopts it to 
Astro-SDK style providing some easy-to-user `transfer_table` and `transfer_tables` functions. 
They are highly customizable and fully support SQL templates.

### Timed dictionaries support

Timed dictionaries are special kind of tables used mostly in Data Warehousing.
They are designed in such a way that they keep multiple values of the same
attribute for given key, and have a timestamp on what attribute value was
at particular time moment.

For example, this is a time dictionary table (PostgreSQL notation is used):

```sql
create table dict_values(
    record_id serial primary key,
    effective_from timestamp not null,
    effective_to timestamp,
    src_id int not null,
    some_value text not null
);
```
Here, `src_id` is an ID of a record on a source, and `some_value` is
time-dependent attribute of that ID. 
And, `effective_from` / `effective_to` pair of columns denotes
what period the particular value of `some_value` was assigned to
particular `src_id` value.

If we have this table filled like this:


| record_id | effective_from | effective_to | src_id | some_value |
| --------- | -------------- | ---------    | ------ | ---------- |
| 1         | 2023-01-11 00:00:00 | null | 10 | Some value |


and then `some_value` property of record with `id = 10` on source changes to `Some other value``, 
then, after updating at 2023-11-02 00:01:00, this timed dictionary should look like this:

| record_id | effective_from | effective_to | src_id | some_value |
| --------- | -------------- | ---------    | ------ | ---------- |
| 1         | 2023-11-01 00:00:01 | 2023-11-02 00:00:59 | 10 | Some value |
| 2         | 2023-11-02 00:01:00 | null | 10 | Some other value |


As you see, record with `record_id = 1` is now "closed", meaning that it
has `effective_to` attribute set, and new record with `record_id = 2` was
added and have `effective_to` set to null.

Any query on this timed dictionarty should include `effective_from` / `effective_to`
columns check, for example like this:

```sql
select src_id as id, some_value from dict_values
where date_of_interest between effective_from and coalesce(effective_to, '2099-12-31')
```

where date_of_interest is some date when this query is running for.

To update timed dictionaries, one should use `update_timed_table` / `update_timed_tables`
functions.

## Usage examples

Create a DAG which opens a session, outputs session info to log and closes it:

``` python
with DAG(...) as dag, ETLSession('source', 'target') as session:
    @dag.task
    def print_session(session: ETLSession):
        print(session)
    print_session(session)
```

Create a DAG with `open-session -> transfer-data_table -> close_session`
task sequence:

``` python
with DAG(...) as dag:
    session = open_session('source', 'target')
    transfer_table('data_table', session=session)
    close_session(session)
```

Upon execution, this DAG will transfer data of `data_table` from source to target
database wrapping this in a session. This would help to easily identify when particular record
was loaded or clean up after unsuccessfull attempts.

The same, but using a context manager:

``` python
with DAG(...) as dag, ETLSession('source', 'target') as session:
    transfer_table('test_table', session=session)
```

## Installation

To build a package from source, run Python build command and then install the package:

``` console
python -m build .
pip install ./dist/astro_extras-xxxx-py3-none-any.whl
```

Airflow could be used directly under Unix environment, but this is not possible in Windows.
Anyway, using a docker is much easier in all cases.

A `docker-compose.yml` file should be used to start Airflow in a docker
environment. After building the package, run this command at the top directory:

``` console
docker compose -p astro-extras up -d --build
```

To shutdown the docker, run:

``` console
docker compose -p astro-extras down -v
```

## Testing

To setup testing evironment, install pytest with:

``` console
pip install -r ./tests/requirements.txt 
```

Then, jump to `/tests` directory and run pytest:

``` console
cd ./tests
pytest -s -v
```

This will build docker image and start `astro-extras-test` stack using `docker-compose-test.yml` file.
The packaget must be built beforehand.

After the tests complete, test stack will be shot down, but you may add `--keep` option
in order to keep it running.
