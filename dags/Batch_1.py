from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

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
    python_callable=lambda: print('Sources to S3 Raw Bucket'),
    dag=dag,
)

task_b = PythonOperator(
    task_id='staging_s3',
    python_callable=lambda: print('S3 Raw Bucket to S3 Staging Bucket in one forced format and Encoding'),
    dag=dag,
)

task_c = PythonOperator(
    task_id='snowflake_raw',
    python_callable=lambda: print('S3 Staging to Snowflake Raw Copy Command'),
    dag=dag,
)

task_d = PythonOperator(
    task_id='snowflake_meta',
    python_callable=lambda: print('Meta Information from S3'),
    dag=dag,
)

task_e = PythonOperator(
    task_id='snowflake_staging',
    python_callable=lambda: print('Merge script to remove duplicates'),
    dag=dag,
)

task_f = PythonOperator(
    task_id='snowflake_reporting',
    python_callable=lambda: print('Transformations performed as per business needs'),
    dag=dag,
)

# Define task dependencies
start_task >> task_a >> task_b >> task_c >> task_d >> task_e >> task_f >> end_task
