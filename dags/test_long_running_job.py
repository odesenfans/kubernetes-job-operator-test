import datetime as dt
from typing import Union

import pendulum
from airflow import DAG
from airflow_kubernetes_job_operator.kubernetes_job_operator import JobRunnerDeletePolicy, KubernetesJobOperator


def create_dag(
    dag_name: str,
    start_date: dt.datetime,
    schedule_interval: Union[dt.datetime, str],
):
    default_args = {
        "owner": "Airflow",
        "start_date": start_date,
        "retries": 0,
    }
    dag = DAG(
        dag_name,
        default_args=default_args,
        description="Test a 'connection reset by peer' issue with K8S jobs.",
        schedule_interval=schedule_interval,
        max_active_runs=1,
    )

    k8s_task = KubernetesJobOperator(
        task_id=f"{dag_name}_task",
        dag=dag,
        body_filepath="dags/dags/templates/long-running-job.yaml",
        namespace="airflow",
        in_cluster=True,
        delete_policy=JobRunnerDeletePolicy.IfSucceeded,
    )

    dag >> k8s_task

    return dag


k8s_dag = create_dag(
    dag_name="test_long_running_job",
    start_date=dt.datetime(2021, 2, 15, tzinfo=pendulum.timezone("Europe/Brussels")),
    schedule_interval="0 6 * * *",
)
