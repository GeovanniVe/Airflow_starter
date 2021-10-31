from datetime import timedelta
from airflow import DAG
import airflow.utils.dates
from custom_modules.s3_to_postgres import S3ToPostgresOperator

default_args = {
    'owner': 'geovanni.velazquez',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['vgeovanni474@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': 1,
    'retry_delay': timedelta(minute=5)
}

dag = DAG('dag_insert_data_postgres', 
          default_args=default_args,
          schedule_interval='@once')

process_data = S3ToPostgresOperator(task_id='dag_s3_to_postgres',
                                    schema='debootcamp',
                                    table='products',
                                    s3_bucket='db-airflow-data',
                                    s3_key='sample.csv',
                                    aws_conn_postgres_id='postgres_default',
                                    aws_conn_id='aws_default',
                                    dag=dag)
