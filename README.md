# Astro SDK extras project

Version 0.0.1 by [kol](skolchin@gmail.com)

I've been using [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/)
as ETL tool for quite some time. Recently I came across
[Astronomer's Astro SDK](https://docs.astronomer.io/astro)
which simplifies data processig with Airflow very much by providing
functionality to easily load or save data,
define custom transformations, adding transparent support of newest
[Dataset](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html)
concept, automatic lineage and so on.

The goal of this project is to add some usefull features to these great products, such as:

* ETL session concept
* Simplified table processing routines
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

ETL session instance is pushed to XCom and becomes available to all operators within
the data pipeline. For example, this SQL query uses data interval defined in ETL session
to limit input data range:

``` sql
select * from data_table
where some_date between '{{ ti.xcom_pull(key="session").period_start }}'::timestamp
                and '{{ ti.xcom_pull(key="session").period_end }}'::timestamp
```

All session management routines are compatible with Airflow's TaskFlow API
providing seamless DAG structure definition using convinient functional programming style.

Usage examples:

Create a DAG which opens a session, outputs session info to log and closes it:

``` python
with DAG(...) as dag, ETLSession('source', 'target') as session:
    @dag.task
    def print_session(session: ETLSession):
        print(session)
    print_session(session)
```

Create a DAG with `open-session -> transfer-test_table -> close_session`
task sequence:

``` python
with DAG(...) as dag:
    session = open_session('source', 'target')
    transfer_table('test_table', session=session)
    close_session(session)
```

Upon execution, this DAG will transfer data of `test_table` from source to target
database wrapping this in a session. This would help to easily identify when particular record
was loaded or clean up after unsuccessfull attempts

### Table processing routines

TBD

## Installation

To build a package from source, run Python build command and then install the package:

``` console
python -m build .
pip install ./dist/astro_extras-0.0.1-py3-none-any.whl
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
