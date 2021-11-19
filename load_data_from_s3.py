from datetime import timedelta
from airflow import DAG
import airflow.utils.dates
import time
time.sleep(5)
import os
from s3_to_postgres import S3ToPostgresOperator
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from airflow.providers.amazon.aws.transfers.google_api_to_s3 import GoogleApiToS3Operator
from airflow.operators.python import PythonOperator


def print_paths():
    import logging
    import sys
    sys.path.append("/opt/airflow/dags/repo/custom_modules")
    logging.info(sys.path)

default_args = {
    'owner': 'geovanni.velazquez',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('dag_insert_data_postgres', 
          default_args=default_args,
          schedule_interval='@once',
          catchup=False,
          tags=['s3_postgres'])

with dag:
    print_path = PythonOperator(task_id="print_params",
                              python_callable=print_paths,
                              provide_context=True,
                              dag=dag)
    names = S3ListOperator(task_id='list_3s_files',
                                    bucket="de-bootcamp-airflow-data", 
                                    prefix='s',
                                    aws_conn_id='aws_default')
    print_path >> names
#     process_data = S3ToPostgresOperator(task_id='dag_s3_to_postgres',
#                                         schema='debootcamp',
#                                         table='products',
#                                         s3_bucket='de-bootcamp-airflow-data',
#                                         s3_key='sample.csv',
#                                         postgres_conn_id='postgres_default',
#                                         aws_conn_id='aws_default',
#                                         dag=dag)

    
    
#     names >> process_data

