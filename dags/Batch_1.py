from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.models import DAG, Variable

import batch_1.tasks.python.raw_s3 as py0
import batch_1.tasks.python.staging_s3 as py1
import batch_1.tasks.python.snowflake_raw as py2
import batch_1.tasks.python.snowflake_meta as py3
import batch_1.tasks.python.snowflake_staging as py4
import batch_1.tasks.python.snowflake_reporting as py5

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Instantiate the DAG
dag = DAG('simple_batch_processing', 
          default_args=default_args,
          description='A simple DAG for batch processing',
          schedule_interval=None)

# Define tasks
start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

task_a = PythonOperator(
    task_id='raw_s3',
    python_callable=py0.raw_s3,
    dag=dag,
)

task_b = PythonOperator(
    task_id='staging_s3',
    python_callable=py1.staging_s3,
    dag=dag,
)

task_c = PythonOperator(
    task_id='snowflake_raw',
    python_callable=py2.snowflake_raw,
    dag=dag,
)

task_d = PythonOperator(
    task_id='snowflake_meta',
    python_callable=py3.snowflake_meta,
    dag=dag,
)

task_e = PythonOperator(
    task_id='snowflake_staging',
    python_callable=py4.snowflake_staging,
    dag=dag,
)

task_f = PythonOperator(
    task_id='snowflake_reporting',
    python_callable=py5.snowflake_reporting,
    dag=dag,
)

# Define task dependencies
start_task >> task_a >> task_b >> task_c >> task_d >> task_e >> task_f >> end_task
