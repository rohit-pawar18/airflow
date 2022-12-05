from datetime import datetime
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 2, 16),
    'email': ['rohit.pawar@biourja.com','rohit18@mailinator.com'],
    'email_on_failure': True,
}

with DAG(dag_id="Sending_mail",schedule_interval="@once",default_args=default_args):
    task_1 = BashOperator(
            task_id='task_1',
            bash_command='cd ..',
            )
    sending_email = EmailOperator(
        task_id='sending_email',
        to='rohit18@mailinator.com',
        subject='Airflow Alert !!!',
        html_content="""<h1>Testing Email using Airflow</h1>""",
        )

    task_2 = BashOperator(
            task_id='task_2',
            bash_command='cd temp_folder',
            )
task_1 >> sending_email >> task_2

