# Astro SDK Extras project
# (c) kol, 2023

import pendulum
from airflow.models import DAG
from astro_extras import run_sql_template, get_template, get_template_file

with DAG(
    dag_id='run-template',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
    tags=['test', 'template'],
) as dag:
    run_sql_template('test', 'source_db')

from jinja2 import Template
from airflow.operators.email import EmailOperator
from airflow.operators.python import get_current_context

with DAG(
    dag_id='send-email',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
    tags=['test', 'template'],
) as dag:
    # @dag.task
    # def get_content():
    #     templ = get_template('mail', '.html', dag=dag)
    #     print(f'Template: {templ}')
    #     content = Template(templ).render(get_current_context())
    #     print(f'Rendered content: {content}')
    #     return content

    # op_content = get_content()
    # EmailOperator(task_id='send-email', 
    #               to='somebody@somewhere.com',
    #               subject='Important!', 
    #               html_content=op_content)

    EmailOperator(
        task_id='send_email', 
        to='somebody@somewhere.com', 
        subject='Important!', 
        html_content=get_template_file('mail', '.html'))
