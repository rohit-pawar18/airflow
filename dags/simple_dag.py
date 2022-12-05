from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain, cross_downstream


#cron tab guru


# def _downloading_data(ds):
#     print("test",ds)
#     return 1

# with DAG(dag_id="simple_dag", schedule_interval=timedelta(days=1), start_date=days_ago(1), catchup=False) as dag:
#     downloading_data = PythonOperator(
#         task_id='downloading_data',
#         python_callable=_downloading_data
#     )
import os
def _downloading_data(ds):
    print("========================writing file==================", os.getcwd())
    with open("/tmp/my.txt",'w') as f:
        print("========================writing file in ==================")
        f.write('mydata')
        print("========================writing file out ==================")

def _checking_data():
    pass

default_args = {
    'owner': 'airflow',
    'email_on_failure': True,
    'email_on_success': True,
    'email': ['rohit.pawar@biourja.com','rohit18@mailinator.com'],
    'retries': 1
}


with DAG(dag_id="aaaaaaaaaaaaa", schedule_interval=timedelta(days=1), default_args=default_args, start_date=days_ago(1), catchup=False) as dag:
    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=_downloading_data
    )
    checking_data = PythonOperator(
        task_id = 'checking_data',
        python_callable=_checking_data
    )

    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id = 'fs_default',
        filepath='my.txt',
    )
    processing_data = BashOperator(
        task_id='processing_data',
        bash_command='exit 0'
    )

    # downloading_data.set_downstream(waiting_for_data)
    # waiting_for_data.set_downstream(processing_data)
    # processing_data.set_upstream(waiting_for_data)
    # waiting_for_data.set_upstream(downloading_data)
    chain(downloading_data ,waiting_for_data, processing_data)
