# Direct

[astro_extras Index](../../../README.md#astro_extras-index) /
`src` /
[Astro Extras](../index.md#astro-extras) /
[Operators](./index.md#operators) /
Direct

> Auto-generated documentation for [src.astro_extras.operators.direct](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/direct.py) module.

- [Direct](#direct)
  - [run_sql_template](#run_sql_template)
  - [run_sql_templates](#run_sql_templates)

## run_sql_template

[Show source in direct.py:20](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/direct.py#L20)

Runs an SQL script from template file.

SQL script is a single SQL statement or multiple statements
to do something good within a single database.

This function renders given template and executes resulting SQL. Templates are
text files with optional macros (see `astro_extras.utils.template.get_template_file`).
Templates are DAG specific.

Normally, only `INSERT`, `UPDATE` and DDL constructs are used,
but scope of statements is not limited. However, no data is ever returned to the caller,
use Astro SDK `run_raw_sql` or `astro_extras.operators.table.load_table` instead if needed.

#### Arguments

- `template` - Template name. This should be a base name of `.sql` file
    located at <dags_home>/templates/<dag.dag_id> folder
- `conn_id` - Connection to database where to execute templated SQL
- `kwargs` - Any extra keyword arguments passed to Astro SDK's `run_raw_sql` function

#### Returns

Task wrapped in `XComArg`

#### Examples

This will execute file `./dags/templates/test_dag/test_template.sql`
on `target_db` connection:

```python
>>> with DAG(dag_id='test_dag', ...) as dag:
>>>     run_sql_template('test_template', 'target_db')
```

#### Signature

```python
def run_sql_template(template: str, conn_id: str, **kwargs) -> XComArg:
    ...
```



## run_sql_templates

[Show source in direct.py:67](https://github.com/skolchin/astro-extras/blob/main/src/astro_extras/operators/direct.py#L67)

 Runs SQL scripts from multiple template files wrapping them up
in Airflow's `TaskGroup`. See [run_sql_template](#run_sql_template) for details.

#### Arguments

- `templates` - List of template names, which are to be base names of `.sql` files
    located at <dags_home>/templates/<dag.dag_id> folder
- `conn_id` - Connection to database where to execute templated SQL
- `group_id` - Optional `TaskGroup` id
- `num_parallel` - Expected number of parallel task sequences within a group.
    For example, if 4 templates are to be processed, and `num_parallel=2`,
    then 4 resulting tasks will be linked pairwise forming 2 separate chains.
- `kwargs` - Any extra keyword arguments passed to Astro SDK's `run_raw_sql` function

#### Returns

Airflow `TaskGroup`

#### Signature

```python
def run_sql_templates(
    templates: Iterable[str],
    conn_id: str,
    group_id: Optional[str] = None,
    num_parallel: Optional[int] = 1,
    **kwargs
) -> TaskGroup:
    ...
```