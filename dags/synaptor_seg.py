"""DAG definition for synaptor basic segmentation workflow (file-backend)."""
import os
from datetime import datetime
from configparser import ConfigParser

from airflow import DAG
from airflow.utils.weight_rule import WeightRule
from airflow.operators.python import PythonOperator
from airflow.models import Variable, BaseOperator as Operator

from worker_op import worker_op
from igneous_and_cloudvolume import check_queue

# from helper_ops import mark_done_op, slack_message_op
from slack_message import task_retry_alert, task_failure_alert, task_done_alert
from helper_ops import scale_up_cluster_op, scale_down_cluster_op
from param_default import default_synaptor_param
from kombu_helper import drain_messages


# hard-coding these for now
MOUNT_POINT = "/root/.cloudvolume/secrets/"
SYNAPTOR_IMAGE = "gcr.io/zetta-lee-fly-vnc-001/synaptor:nazgul"
TASK_QUEUE_NAME = "synaptor"

# Processing parameters
# We need to set a default so any airflow container can parse the dag before we
# pass in a configuration file

try:
    param = Variable.get("synaptor_param", default_synaptor_param)
    cp = ConfigParser()
    cp.read_string(param)
    MAX_CLUSTER_SIZE = cp.getint("Workflow", "maxclustersize")

except:  # not sure why this doesn't work sometimes
    MAX_CLUSTER_SIZE = 1


# Python callables (for PythonOperators)
# These aren't critical, so they're just placeholders for now
def generate_ngl_link_op() -> None:
    """Generates a neuroglancer link to view the results."""
    pass


def drain_op(task_queue_name: str) -> None:
    """Drains leftover messages from the RabbitMQ."""
    from airflow import configuration as conf
    broker_url = conf.get("celery", "broker_url")

    drain_messages(broker_url, task_queue_name)


# Op functions
def sanity_check_op(dag: DAG, queue: str) -> Operator:
    """Checks the uploaded processing parameters for problems."""
    config_path = os.path.join(MOUNT_POINT, "synaptor_param")
    command = f"sanity_check {config_path}"

    # these variables will be mounted in the containers
    variables = ["synaptor_param"]

    return worker_op(
        variables=variables,
        mount_point=MOUNT_POINT,
        task_id="sanity_check",
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


# DAG creation
generator_default_args = {
    "owner": "seuronbot",
    "depends_on_past": False,
    "start_date": datetime(2022, 2, 22),
    "catchup": False,
    "retries": 0,
}

# "update synaptor params"
dag_sanity = DAG(
    "synaptor_sanity_check",
    default_args=generator_default_args,
    schedule_interval=None,
    tags=["synaptor"],
)

# "run synaptor file segmentation"
dag_file_seg = DAG(
    "synaptor_file_seg",
    default_args=generator_default_args,
    schedule_interval=None,
    tags=["synaptor"],
)

# "kill synaptor workers"
dag_shutdown = DAG(
    "synaptor_shutdown",
    default_args=generator_default_args,
    schedule_interval=None,
    tags=["synaptor"],
)

# Instantiating ops within DAGs
## Meta-stuff (cluster management & checks)
drain = PythonOperator(
    task_id="drain_messages",
    python_callable=drain_op,
    priority_weight=100_000,
    op_args=(TASK_QUEUE_NAME,),
    weight_rule=WeightRule.ABSOLUTE,
    on_failure_callback=task_failure_alert,
    on_success_callback=task_done_alert,
    queue="manager",
    dag=dag_file_seg,
)

sanity_check = sanity_check_op(dag_sanity, "manager")
sanity_check_before_run = sanity_check_op(dag_file_seg, "manager")

# generate_ngl_link = PythonOperator(
#     task_id="generate_ng_link",
#     python_callable=generate_ngl_link_op,
#     priority_weight=100_000,
#     weight_rule=WeightRule.ABSOLUTE,
#     queue="manager",
#     dag=dag_file_seg,
# )

scale_up_cluster = scale_up_cluster_op(
    dag_file_seg, TASK_QUEUE_NAME, "synaptor-cpu", 1, MAX_CLUSTER_SIZE, "cluster"
)
scale_down_cluster = scale_down_cluster_op(
    dag_file_seg, TASK_QUEUE_NAME, "synaptor-cpu", 0, "cluster"
)

## Actual work ops
workers = [synaptor_op(dag_file_seg, i) for i in range(MAX_CLUSTER_SIZE)]

generate_chunk_ccs = generate_op(dag_file_seg, "chunk_ccs")
wait_chunk_ccs = wait_op(dag_file_seg, "chunk_ccs")

generate_merge_ccs = generate_op(dag_file_seg, "merge_ccs")
wait_merge_ccs = wait_op(dag_file_seg, "merge_ccs")

generate_remap = generate_op(dag_file_seg, "remap")
wait_remap = wait_op(dag_file_seg, "remap")

generate_kill = generate_op(dag_file_seg, "self_destruct")
generate_kill_shutdown = generate_op(dag_shutdown, "self_destruct")


# Setting up dependencies
## Sanity check dag
(
    sanity_check
    # >> sanity_check_report
)

## Drain old tasks before doing anything
drain >> sanity_check_before_run

## Worker dag
(sanity_check_before_run >> scale_up_cluster >> workers >> scale_down_cluster)

## Generator/task dag
(
    sanity_check_before_run
    # >> sanity_check_report
    >> generate_chunk_ccs
    >> wait_chunk_ccs
    >> generate_merge_ccs
    >> wait_merge_ccs
    >> generate_remap
    >> wait_remap
    >> generate_kill
    # >> generate_ngl_link
)
