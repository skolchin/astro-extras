# Astro SDK Extras project
# (c) kol, 2023

""" Template utility functions """

import os
import jinja2
import importlib.resources
from airflow.models import DAG
from airflow.models.dag import DagContext
from airflow.configuration import conf
from airflow.exceptions import AirflowFailException

def get_template_file(
        template: str, 
        ext: str = '.sql', 
        dag: DAG | None = None,
        fail_if_not_found: bool = False) -> str | None:
    
    """ Returns reference to DAG-specific template file, if one exists.
    
    This function looks up for a template file in `./templates/<dag_id>` folder
    under directory set as DAGS_HOME in Airflow config (usually `./dags`).

    Templates are used for SQL operations, html reporting and so on.
    They consists of some plain-text content mixed with Jinja macros inside `{{...}}` brackets,
    which will be substituted ('rendered`) with some variables either by Airflow runtime or manually.

    Args:
        template:   Template name (e.g. table name or reporting object) without any extension
        ext:        Template file extension, `.sql` by default
        dag:        Optional DAG context. If not provided, current DAG context will be used
        fail_if_not_found: If `True`, `AirflowFailException` will be raised if template does not exist
            (default is `False`)

    Returns:
        Name of template file relative to Airflow's `DAG_HOME` folder or `None` if no file was found
        and `fail_if_not_found = False`

    Examples:
        If a `test_table` is to be processed in DAG with `test-transfer` ID, 
        this SQL template could be used to add required `session_id` field 
        at the beginning of a target table:

            select {{ti.xcom_pull(key="session").session_id}} as session_id, * 
            from test_table

        The template must be located in `./dags/templates/test-transfer/test_table.sql` file.

        Then, during this DAG execution, the template will automatically be picked up and executed,
        substituting macros with actual `session_id` value:

        >>> with DAG(dag_id='test-transfer', ...) as dag:
        >>>     session = open_session('source', 'target')
        >>>     transfer_table('test_table', session=session)
        >>>     close_session(session)
    """

    dag = dag or DagContext.get_current_dag()
    if dag is None:
        if fail_if_not_found:
            raise AirflowFailException('Cannot retrieve DAG context')
        return None
    
    ext = '.' + ext if not ext.startswith('.') else ext
    rel_file_name = os.path.join('templates', dag.dag_id, template + ext)
    full_file_name = os.path.join(conf.get('core','dags_folder'), rel_file_name)
    if os.path.exists(full_file_name):
        return rel_file_name
    
    if fail_if_not_found:
        raise AirflowFailException(f'Template file {full_file_name} was not found')
    return None

def get_template(
        template: str, 
        ext: str = '.sql', 
        dag: DAG | None = None,
        fail_if_not_found: bool = False,
        read_mode: str = 'rt') -> str | bytes | None:
    
    """ Returns template content. See `get_template_file` for details on templates.

    Args:
        template:   Template name (e.g. table name or reporting object) without any extension
        ext:        Template file extension, `.sql` by default
        dag:        Optional DAG context. If not provided, current DAG context will be used
        fail_if_not_found: If `True`, `AirflowFailException` will be raised if template does not exist
        read_mode:  Read mode (either `rt` for text or `rb` for binary)

    Returns:
        Template file content or `None` if no file was found and `fail_if_not_found = False`.

    Examples:
        Load template from `./dags/templates/test_dag/mail.html`, manually render it
        with Airflow DAG context and send it as a mail:

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

        Note that because `EmailOperator.html_content` is a templated field,
        Airflow will automatically render it, so this example might even more be shortened:

        >>> from airflow.operators.email import EmailOperator
        >>> from airflow.operators.python import get_current_context
        >>>
        >>> with DAG(dag_id='test_dag', ...) as dag:
        >>>     EmailOperator(task_id='send_email', to='somebody@somewhere.com', 
        >>>         subject='Important!', 
        >>>         html_content=get_template_file('mail', '.html'))
        
    """
    assert read_mode in ('rt', 'rb'), f'Read_mode must be either `rt` or `rb`'
    template_file = get_template_file(template, ext, dag, fail_if_not_found)
    if not template_file:
        return None
    full_file_name = os.path.join(conf.get('core','dags_folder'), template_file)
    with open(full_file_name, read_mode) as fp:
        return fp.read()

def get_predefined_template(name: str) -> jinja2.Template:
    """ Retrieves a template from package resources. Mostly for internal use. """
    template_file = f'templates/{name}'
    template_res = importlib.resources.files('astro_extras').joinpath(template_file)
    with importlib.resources.as_file(template_res) as template_file:
        return jinja2.Template(template_file.read_text())
