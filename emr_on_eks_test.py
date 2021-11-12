"""
This is an example dag for an Amazon EMR on EKS Spark job.
"""
import os
from datetime import timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_containers import EMRContainerOperator
from airflow.utils.dates import days_ago

os.environ["AWS_DEFAULT_REGION"] = "us-east-2"

# [START howto_operator_emr_eks_env_variables]
VIRTUAL_CLUSTER_ID = "k7lgg23squ1o1hgjfrlfh7oqt"  # os.getenv("VIRTUAL_CLUSTER_ID", "virtual_cluster_test")
JOB_ROLE_ARN = os.getenv("JOB_ROLE_ARN", "arn:aws:iam::855157247171:role/aws-service-role/emr-containers.amazonaws.com/AWSServiceRoleForAmazonEMRContainers")
# [END howto_operator_emr_eks_env_variables]


# [START howto_operator_emr_eks_config]
JOB_DRIVER_ARG = {
    "sparkSubmitJobDriver": {
        "entryPoint": "local:///usr/lib/spark/examples/src/main/python/wordcount.py",
        "sparkSubmitParameters": "--conf spark.executors.instances=1 --conf spark.executors.memory=1G --conf spark.executor.cores=1 --conf spark.driver.cores=1",  # noqa: E501
    }
}

CONFIGURATION_OVERRIDES_ARG = {
    "applicationConfiguration": [
        {
            "classification": "spark-defaults",
            "properties": {
                "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",  # noqa: E501
            },
        }
    ],
    "monitoringConfiguration": {
        "cloudWatchMonitoringConfiguration": {
            "logGroupName": "/aws/emr-eks-spark",
            "logStreamNamePrefix": "airflow",
        }
    },
}
# [END howto_operator_emr_eks_config]

with DAG(
    dag_id='emr_eks_pi_job',
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["emr_containers", "example"],
) as dag:

    # An example of how to get the cluster id and arn from an Airflow connection
    # VIRTUAL_CLUSTER_ID = '{{ conn.emr_eks.extra_dejson["virtual_cluster_id"] }}'
    # JOB_ROLE_ARN = '{{ conn.emr_eks.extra_dejson["job_role_arn"] }}'

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
