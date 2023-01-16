from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

with DAG(
    "spark_dag",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="A simple spark DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 16),
    catchup=False,
    tags=["spark", "pi"],
) as dag:

    # use spark-on-k8s to operate against the data
    spark_job = SparkKubernetesOperator(
        task_id="spark_job",
        namespace="processing",
        application_file="./spark-py.yaml",
        do_xcom_push=True
    )

    # monitor spark application
    spark_job_sensor = SparkKubernetesSensor(
        task_id=f'spark_job_sensor',
        namespace="processing",
        application_name="{{ task_instance.xcom_pull(task_ids='spark_job')['metadata']['name'] }}",
        attach_log=True
    )

    spark_job >> spark_job_sensor