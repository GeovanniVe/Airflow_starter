from datetime import timedelta
from airflow import DAG
import airflow.utils.dates

import sys
sys.path.append("/opt/airflow/dags/repo/custom_modules")

from airflow.providers.amazon.aws.operators.emr_containers import EMRContainerOperator
from s3_to_postgres import S3ToPostgresOperator
from postgres_to_s3 import PostgresToS3Operator
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from airflow.operators.python import PythonOperator
import os
os.environ["AWS_DEFAULT_REGION"] = "us-east-2"

# [START howto_operator_emr_eks_env_variables]
VIRTUAL_CLUSTER_ID = "knk7mbdpb9m63ockfv5e59pg1"  # os.getenv("VIRTUAL_CLUSTER_ID", "virtual_cluster_test")
JOB_ROLE_ARN = "arn:aws:iam::855157247171:role/EMRContainers-JobExecutionRole"
# [END howto_operator_emr_eks_env_variables]


# [START howto_operator_emr_eks_config]
JOB_DRIVER_ARG = {
    "sparkSubmitJobDriver": {
        "entryPoint": "s3://spark-test-samp/classify_reviews.py",
        "sparkSubmitParameters": "--conf spark.executors.instances=2"
                                 " --conf spark.executors.memory=2G"
                                 " --conf spark.executor.cores=2"
                                 " --conf spark.driver.cores=1"
    }
}

CONFIGURATION_OVERRIDES_ARG = {
    "applicationConfiguration": [
        {
            "classification": "spark-defaults",
            "properties": {
              "spark.dynamicAllocation.enabled": "false",
              "spark.kubernetes.executor.deleteOnTermination": "true",
              "spark.kubernetes.container.image": "855157247171.dkr.ecr.us-east-2.amazonaws.com/emr6.3_custom_repo",
              "spark.hadoop.fs.s3a.multiobjectdelete.enable": "false"
            }
        }
    ],
    "monitoringConfiguration": {
        "cloudWatchMonitoringConfiguration": {
            "logGroupName": "/emr-on-eks/eksworkshop-eksctl",
            "logStreamNamePrefix": "pi"
        },
        "s3MonitoringConfiguration": {
            "logUri": "s3://spark-test-samp"
        }
    }    
}
# [END howto_operator_emr_eks_config]


def print_params_fn(**kwargs):
    import logging
    import os
    logging.info("getting virtual cluster ID: {0}".format(os.getenv("VIRTUAL_CLUSTER_ID", "virtual_cluster_test")))
    logging.info("getting virtual cluster ID 2: {0}".format(os.getenv("VIRTUAL_CLUSTER_ID", "airflow-eks-data-bootcamp")))
    logging.info("{0}".format(os.environ))
    return None


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
    print_env = PythonOperator(task_id="print_params",
                              python_callable=print_paths,
                              provide_context=True,
                              dag=dag)
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
    
    pg_to_staging = PostgresToS3Operator(task_id='dag_postgres_to_s3',
                                        schema='debootcamp',
                                        table='products',
                                        s3_bucket='de-bootcamp-airflow-data',
                                        s3_key='sample.csv',
                                        postgres_conn_id='postgres_default',
                                        aws_conn_id='aws_default',
                                        dag=dag)

    
    print_env
    names >> process_data >> pg_to_staging


    # An example of how to get the cluster id and arn from an Airflow connection
    # VIRTUAL_CLUSTER_ID = '{{ conn.emr_eks.extra_dejson["virtual_cluster_id"] }}'
    # JOB_ROLE_ARN = '{{ conn.emr_eks.extra_dejson["job_role_arn"] }}'
    
    print_path = PythonOperator(task_id="print_path",
                              python_callable=print_params_fn,
                              provide_context=True,
                              dag=dag)
    
    # [START howto_operator_emr_eks_jobrun]
    job_starter = EMRContainerOperator(
        task_id="start_job",
        execution_role_arn=JOB_ROLE_ARN,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        release_label="emr-6.3.0-latest",
        job_driver=JOB_DRIVER_ARG,
        name="movie_reviews.py",
    )
    # [END howto_operator_emr_eks_jobrun]
    
    print_path >> job_starter
