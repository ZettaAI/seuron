"""Operator functions for synaptor DAGs."""
import os

from airflow import DAG
from airflow.utils.weight_rule import WeightRule
from airflow.operators.python import PythonOperator
from airflow.models import Variable, BaseOperator as Operator

from worker_op import worker_op
from igneous_and_cloudvolume import check_queue

# from helper_ops import mark_done_op, slack_message_op
from slack_message import task_retry_alert, task_failure_alert, task_done_alert
from kombu_helper import drain_messages


# hard-coding these for now
MOUNT_POINT = "/root/.cloudvolume/secrets/"
SYNAPTOR_IMAGE = "gcr.io/zetta-lee-fly-vnc-001/synaptor:nazgul"
TASK_QUEUE_NAME = "synaptor"


# Python callables (for PythonOperators)
def generate_ngl_link_op() -> None:
    """Generates a neuroglancer link to view the results."""
    pass


# Op functions
def drain_op(
    dag: DAG, task_queue_name: str = TASK_QUEUE_NAME, queue: str = "manager",
) -> Operator:
    """Drains leftover messages from the RabbitMQ."""
    from airflow import configuration as conf

    broker_url = conf.get("celery", "broker_url")

    return PythonOperator(
        task_id="drain_messages",
        python_callable=drain_messages,
        priority_weight=100_000,
        op_args=(broker_url, task_queue_name),
        weight_rule=WeightRule.ABSOLUTE,
        on_failure_callback=task_failure_alert,
        on_success_callback=task_done_alert,
        queue="manager",
        dag=dag,
    )


def manager_op(dag: DAG, synaptor_task_name: str, queue: str = "manager") -> Operator:
    """An operator fn for running synaptor tasks on the airflow node."""
    config_path = os.path.join(MOUNT_POINT, "synaptor_param")
    command = f"{synaptor_task_name} {config_path}"

    # these variables will be mounted in the containers
    variables = ["synaptor_param"]

    return worker_op(
        variables=variables,
        mount_point=MOUNT_POINT,
        task_id=synaptor_task_name,
        command=command,
        force_pull=True,
        on_failure_callback=task_failure_alert,
        on_success_callback=task_done_alert,
        image=SYNAPTOR_IMAGE,
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=queue,
        dag=dag,
    )


def generate_op(
    dag: DAG,
    taskname: str,
    op_queue_name: str = "manager",
    task_queue_name: str = TASK_QUEUE_NAME,
) -> Operator:
    """Generates tasks to run and adds them to the RabbitMQ."""
    from airflow import configuration as conf

    broker_url = conf.get("celery", "broker_url")
    config_path = os.path.join(MOUNT_POINT, "synaptor_param")

    command = (
        f"generate {taskname} {config_path}"
        f" --queueurl {broker_url}"
        f" --queuename {task_queue_name}"
    )

    # these variables will be mounted in the containers
    variables = add_secrets_if_defined(["synaptor_param"])

    return worker_op(
        variables=variables,
        mount_point=MOUNT_POINT,
        task_id=f"generate_{taskname}",
        command=command,
        force_pull=True,
        on_failure_callback=task_failure_alert,
        on_success_callback=task_done_alert,
        image=SYNAPTOR_IMAGE,
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=op_queue_name,
        dag=dag,
    )


def synaptor_op(
    dag: DAG,
    i: int,
    op_queue_name: str = "synaptor-cpu",
    task_queue_name: str = TASK_QUEUE_NAME,
) -> Operator:
    """Runs a synaptor worker until it receives a kill task."""
    from airflow import configuration as conf

    broker_url = conf.get("celery", "broker_url")
    config_path = os.path.join(MOUNT_POINT, "synaptor_param")

    command = (
        f"worker --configfilename {config_path}"
        f" --queueurl {broker_url} "
        f" --queuename {task_queue_name}"
        " --lease_seconds 300"
    )

    # these variables will be mounted in the containers
    variables = add_secrets_if_defined(["synaptor_param"])

    return worker_op(
        variables=variables,
        mount_point=MOUNT_POINT,
        task_id=f"worker_{i}",
        command=command,
        force_pull=True,
        on_retry_callback=task_retry_alert,
        image=SYNAPTOR_IMAGE,
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=op_queue_name,
        dag=dag,
        qos=False,  # turns off a 5 minute failure timer
    )


def wait_op(dag: DAG, taskname: str) -> Operator:
    """Waits for a task to finish. Not used until we use RabbitMQ."""
    return PythonOperator(
        task_id=f"wait_for_queue_{taskname}",
        python_callable=check_queue,
        op_args=(TASK_QUEUE_NAME,),
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        on_success_callback=task_done_alert,
        queue="manager",
        dag=dag,
    )


# Helper functions
def add_secrets_if_defined(variables: list[str]) -> list[str]:
    """Adds CloudVolume secret files to the mounted variables if defined."""
    maybe_aws = Variable.get("aws-secret.json", None)
    maybe_gcp = Variable.get("google-secret.json", None)

    if maybe_aws is not None:
        variables.append("aws-secret.json")
    if maybe_gcp is not None:
        variables.append("google-secret.json")

    return variables
