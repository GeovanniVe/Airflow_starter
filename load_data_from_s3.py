from datetime import timedelta
from airflow import DAG
import airflow.utils.dates
from custom_modules.s3_to_postgres import S3ToPostgresOperator
from airflow.contrib.operators.s3_list_operator import S3ListOperator
import logging


default_args = {
    'owner': 'geovanni.velazquez',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('dag_insert_data_postgres', 
          default_args=default_args,
          schedule_interval='@once')

with dag:
    names = S3ListOperator(task_id='list_3s_files',
                                    bucket="de-bootcamp-airflow-data", 
                                    prefix='s',
                                    aws_conn_id='aws_default')
    
    process_data = S3ToPostgresOperator(task_id='dag_s3_to_postgres',
                                        schema='debootcamp',
                                        table='products',
                                        s3_bucket='de-bootcamp-airflow-data',
                                        s3_key=names.xcom_pull(task_ids='list_3s_files'),
                                        postgres_conn_id='postgres_default',
                                        aws_conn_id='aws_default',
                                        dag=dag)

    
    
    names >> process_data

