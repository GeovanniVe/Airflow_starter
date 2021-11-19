"""
This is an example dag for an Amazon EMR on EKS Spark job.
"""
import os
from datetime import timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_containers import EMRContainerOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

os.environ["AWS_DEFAULT_REGION"] = "us-east-2"

# [START howto_operator_emr_eks_env_variables]
VIRTUAL_CLUSTER_ID = "twckapogtyv41lnsiwwsw23bw"  # os.getenv("VIRTUAL_CLUSTER_ID", "virtual_cluster_test")
JOB_ROLE_ARN = "arn:aws:iam::855157247171:role/EMRContainers-JobExecutionRole"
# [END howto_operator_emr_eks_env_variables]


# [START howto_operator_emr_eks_config]
JOB_DRIVER_ARG = {
    "sparkSubmitJobDriver": {
        "entryPoint": "s3://spark-test-samp/spark_ex.py",
        "sparkSubmitParameters": "--conf spark.executors.instances=2 --conf spark.executors.memory=2G --conf spark.executor.cores=2 --conf spark.driver.cores=1",  # noqa: E501
    }
}

CONFIGURATION_OVERRIDES_ARG = {
    "applicationConfiguration": [
        {
            "classification": "spark-defaults",
            "properties": {
              "spark.dynamicAllocation.enabled": "false",
              "spark.kubernetes.executor.deleteOnTermination": "true",
              "spark.kubernetes.container.image": "855157247171.dkr.ecr.us-east-2.amazonaws.com/emr6.3_custom_repo"
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
    logging.info("getting virtual cluster ID: {0}".format(os.getenv("VIRTUAL_CLUSTER_ID", "virtual_cluster_test")))
    logging.info("getting virtual cluster ID 2: {0}".format(os.getenv("VIRTUAL_CLUSTER_ID", "airflow-eks-data-bootcamp")))
    logging.info("{0}".format(os.environ))
    return None


with DAG(
    dag_id='emr_eks_pi_job',
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["emr_containers", "example"],
    catchup=False
) as dag:

    # An example of how to get the cluster id and arn from an Airflow connection
    # VIRTUAL_CLUSTER_ID = '{{ conn.emr_eks.extra_dejson["virtual_cluster_id"] }}'
    # JOB_ROLE_ARN = '{{ conn.emr_eks.extra_dejson["job_role_arn"] }}'
    
    print_params = PythonOperator(task_id="print_params",
                              python_callable=print_params_fn,
                              provide_context=True,
                              dag=dag)
    
    # [START howto_operator_emr_eks_jobrun]
    job_starter = EMRContainerOperator(
        task_id="start_job",
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        execution_role_arn=JOB_ROLE_ARN,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        release_label="emr-6.3.0-latest",
        job_driver=JOB_DRIVER_ARG,
        name="pi.py",
    )
    # [END howto_operator_emr_eks_jobrun]
    
    print_params >> job_starter
