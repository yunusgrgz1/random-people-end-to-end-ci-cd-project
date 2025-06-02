from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from get_data import get_and_upload_to_s3
from upload_to_postgres import load_to_postgres


logger = logging.getLogger('dag_logger')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025,6,1)
}

dag = DAG(
    dag_id="random_name_api_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup= False,
    max_active_runs=1
)

def start_job():
    logger.info("Starting the people data processing pipeline.")
    
def fetch_and_upload_to_s3():
    logger.info("Getting data from the API and uploading to S3")
    get_and_upload_to_s3()
    
def upload_to_postgres():
    logger.info("Uploading to postgres")
    load_to_postgres()

def end_data_job():
    logger.info("Data processing pipeline finished.")   
    
    
start_task = PythonOperator(
    task_id='start_job',
    python_callable=start_job,
    dag=dag
)

getting_data_task = PythonOperator(
    task_id='getting_and_uploading_job',
    python_callable=fetch_and_upload_to_s3,
    dag=dag
)

uploading_to_postgres_task = PythonOperator(
    task_id='uploading_to_postgres_job',
    python_callable=upload_to_postgres,
    dag=dag
)

end_task = PythonOperator(
    task_id='end_data_job',
    python_callable=end_data_job,
    dag=dag
)

start_task >> getting_data_task >> uploading_to_postgres_task >> end_task