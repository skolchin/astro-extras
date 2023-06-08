# Template

[Astro-sdk-extra Index](../README.md#astro-sdk-extra-index) /
[Utils](./index.md#utils) /
Template

> Auto-generated documentation for [utils.template](../../src/astro_extras/utils/template.py) module.

- [Template](#template)
  - [get_template](#get_template)
  - [get_template_file](#get_template_file)

## get_template

[Show source in template.py:69](../../src/astro_extras/utils/template.py#L69)

Returns template content. See [get_template_file](#get_template_file) for details on templates.

#### Arguments

- `template` - Template name (e.g. table name or reporting object) without any extension
- `ext` - Template file extension, `.sql` by default
- `dag` - Optional DAG context
- `fail_if_not_found` - If `True`, `AirflowFailException` will be raised if template does not exist

#### Returns

Template file content or `None` if no file was found and `fail_if_not_found = False`

#### Examples

Load template from `./dags/templates/test_dag/mail.html`, manually render it
with Airflow DAG context and send it as a mail:

```python
>>> from jinja2 import Template
>>> from airflow.operators.email import EmailOperator
>>> from airflow.operators.python import get_current_context
>>>
>>> with DAG(dag_id='test_dag', ...) as dag:
>>>     @dag.task
>>>     def get_content():
>>>         templ = get_template('mail', '.html', dag=dag)
>>>         return Template(templ).render(get_current_context())
>>>
>>>     EmailOperator(task_id='send_email', to='somebody@somewhere.com',
>>>         subject='Important!', html_content=get_content())
```

Note that because `EmailOperator.html_content` is a templated field,
Airflow will automatically render it, so this example might even more be shortened:

```python
>>> from airflow.operators.email import EmailOperator
>>> from airflow.operators.python import get_current_context
>>>
>>> with DAG(dag_id='test_dag', ...) as dag:
>>>     EmailOperator(task_id='send_email', to='somebody@somewhere.com',
>>>         subject='Important!',
>>>         html_content=get_template_file('mail', '.html'))
```

#### Signature

```python
def get_template(
    template: str,
    ext: Optional[str] = ".sql",
    dag: Optional[DAG] = None,
    fail_if_not_found: Optional[bool] = False,
) -> Optional[str]:
    ...
```



## get_template_file

[Show source in template.py:14](../../src/astro_extras/utils/template.py#L14)

Returns reference to DAG-specific template file, if one exists.

This function looks up for a template file in `./templates/<dag_id>` folder
under directory set as DAGS_HOME in Airflow config (usually `./dags`).

Templates are used for SQL operations, html reporting and so on.
They consists of some plain-text content mixed with Jinja macros inside `{{...}}` brackets,
which will be substituted ('rendered`) with some variables either by Airflow runtime or manually.

#### Arguments

- `template` - Template name (e.g. table name or reporting object) without any extension
- `ext` - Template file extension, `.sql` by default
- `dag` - Optional DAG context
- `fail_if_not_found` - If `True`, `AirflowFailException` will be raised if template does not exist
    (default is `False`)

#### Returns

Name of template file relative to Airflow's `DAG_HOME` folder or `None` if no file was found
and `fail_if_not_found = False`

#### Examples

If a `test_table` is to be processed in DAG with `test-transfer` ID,
this SQL template could be used to add required `session_id` field
at the beginning of a target table:

select {{ti.xcom_pull(key="session").session_id}} as session_id, *
from test_table

The template must be located in `./dags/templates/test-transfer/test_table.sql` file.

Then, during this DAG execution, the template will automatically be picked up and executed,
substituting macros with actual `session_id` value:

```python
>>> with DAG(dag_id='test-transfer', ...) as dag:
>>>     session = open_session('source', 'target')
>>>     transfer_table('test_table', session=session)
>>>     close_session(session)
```

#### Signature

```python
def get_template_file(
    template: str,
    ext: Optional[str] = ".sql",
    dag: Optional[DAG] = None,
    fail_if_not_found: Optional[bool] = False,
) -> Optional[str]:
    ...
```