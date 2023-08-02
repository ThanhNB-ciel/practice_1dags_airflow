from datetime import datetime, timedelta
from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from clickhouse_driver import Client
from airflow import DAG
from utils import *
from clickhouse_driver import Client
import datetime
from datetime import datetime



yes = datetime.today() - timedelta(1)

with DAG(
    'crawl_ck',
    default_args= {
        'email' : ['thanhnb@ftech.ai'],
        'email_on_failure': True,
        'retries' : 1,
        'retry_delay' : timedelta(minutes=3),
    },
    description='time_crawl',
    schedule_interval= "0 8 * * 1-5",
    start_date= datetime(yes.year, yes.month, yes.day),
    tags=['thanhnb_crawl'],
    
) as dag:
    
    task_crawl = PythonOperator(
        task_id="crawl_data",
        python_callable= crawl_ck,
        # provide_context = True
        
    )
    task_minio = PythonOperator(
        task_id = 'task_minio',
        python_callable= inser_minio,
        # provide_context = True
    )
    tash_insert = PythonOperator( 
        task_id='insert_clickhouse',
        python_callable= insert_data,
        # provide_context = True
    )

task_crawl >> task_minio >> tash_insert 

