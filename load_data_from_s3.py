from datetime import timedelta
from airflow import DAG
import airflow.utils.dates
from custom_modules.s3_to_postgres import S3ToPostgresOperator
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from airflow.providers.amazon.aws.transfers.google_api_to_s3 import GoogleApiToS3Operator
import os

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "dags/repo/client_secret_871737823530-2ecrb294lru9cih4de5esno3corn6oo0.apps.googleusercontent.com.json"

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
    task_google_sheets_values_to_s3 = GoogleApiToS3Operator(
        google_api_service_name='drive',
        google_api_service_version='v3',
        google_api_endpoint_path='drive.files.get',
        google_api_endpoint_params={'fileId': "1nj2AXJG10DTjSjL0J17WlDdZeunJQ9r6"},
        s3_destination_key="de-bootcamp-airflow-data",
        task_id='google_drive_get_to_s3',
        dag=dag,
    )
    
    names = S3ListOperator(task_id='list_3s_files',
                                    bucket="de-bootcamp-airflow-data", 
                                    prefix='s',
                                    aws_conn_id='aws_default')
    
    process_data = S3ToPostgresOperator(task_id='dag_s3_to_postgres',
                                        schema='debootcamp',
                                        table='products',
                                        s3_bucket='de-bootcamp-airflow-data',
                                        s3_key='sample.csv',
                                        postgres_conn_id='postgres_default',
                                        aws_conn_id='aws_default',
                                        dag=dag)

    
    
    task_google_sheets_values_to_s3 >> names >> process_data

