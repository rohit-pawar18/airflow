from datetime import datetime
from airflow import DAG
from bu_snowflake import get_engine

from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 2, 16),
    'email': ['rohit.pawar@biourja.com','rohit18@mailinator.com'],
    'email_on_failure': True,
}

def connect_sf(x):
    eng = get_engine()
    print("============================", eng)
    sql = '''select * from POWERDB.PQUANT.FPT_RUN_LOG T limit 1'''    
    res = eng.execute(sql)
    print("=================", res.fetchall())
    return x + " is a must have tool for Data Engineers."

with DAG(dag_id="snowflake",schedule_interval="@once",default_args=default_args):

    task_1 = PythonOperator(
            task_id='snowfale_engine',
            python_callable= connect_sf,
            op_kwargs = {"x" : "Apache Airflow"},
            
    )
    sending_email = EmailOperator(
        task_id='sending_email',
        to='rohit18@mailinator.com,rohit.pawar@biourja.com,indiapowerit@biourja.com',
        subject='Airflow Alert !!!',
        html_content="""<h1>Testing Email & snowflake connection.. RP Rocks.</h1>""",
        )

    task_2 = BashOperator(
            task_id='task_2',
            bash_command='cd temp_folder',
            )
task_1 >> sending_email >> task_2