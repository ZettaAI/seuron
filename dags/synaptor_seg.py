"""DAG definition for synaptor basic segmentation workflow (file-backend).

The entire DAG consists of two sub dags (similar to chunkflow_dag.py):
* dag_generator specifies the work for a node that generates tasks and monitors
their completion through the RabbitMQ
* dag_worker specifies how to manage the cluster of worker nodes. This becomes
  "scale up, wait until the workflow is finished, report results, and scale
  down".
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.utils.weight_rule import WeightRule
from airflow.operators.python import PythonOperator
from airflow.models import Variable, BaseOperator as Operator

from worker_op import worker_op
from igneous_and_cloudvolume import check_queue
# from helper_ops import mark_done_op, slack_message_op
from slack_message import task_retry_alert, task_failure_alert
from helper_ops import scale_up_cluster_op, scale_down_cluster_op
from param_default import synaptor_param_default  # , check_worker_image_labels
from param_default import default_args, default_mount_path


# hard-coding these for now
MAX_CLUSTER_SIZE = 10
MOUNT_PATH = "/root/cloudvolume/.secrets"
SYNAPTOR_IMAGE = "nicholasturner/synaptor:nazgul"
TASK_QUEUE_NAME = "synaptor"
default_synaptor_workspace = "/Synaptor/scripts"

# Processing parameters
# need to set a default so any airflow container can parse the dag (?)
Variable.setdefault("synaptor_param", "")
param = Variable.get("synaptor_param", deserialize_json=False)


# Python callables (for PythonOperators)
# These aren't critical, so they're just placeholders for now
def generate_ngl_link_op() -> None:
    """Generates a neuroglancer link to view the results."""
    pass


def check_image_labels_op(junk) -> None:
    """A placeholder since the image labels aren't setup.

    Will replace with param_default.check_worker_image_labels later.
    """
    pass


# Op functions
def sanity_check_op(dag: DAG, queue: str) -> Operator:
    """Checks the uploaded processing parameters for problems."""
    config_path = os.path.join(MOUNT_PATH, "synaptor_param")
    command = f"sanity_check {config_path}"

    # these variables will be mounted in the containers
    variables = ["synaptor_param"]

    return worker_op(
               variables=variables, mount_point=MOUNT_PATH,
               task_id="sanity_check", command=command,
               force_pull=True, on_failure_callback=task_failure_alert,
               image=SYNAPTOR_IMAGE, priority_weight=100_000,
               weight_rule=WeightRule.ABSOLUTE, queue=queue, dag=dag)


def generate_op(dag: DAG,
                taskname: str,
                op_queue_name: str = "manager",
                task_queue_name: str = TASK_QUEUE_NAME,
                ) -> Operator:
    """Generates tasks to run and adds them to the RabbitMQ."""
    from airflow import configuration as conf
    broker_url = conf.get("celery", "broker_url")

    command = (f"generate {taskname} {config_path}"
               f" {broker_url} {task_queue_name}")

    # these variables will be mounted in the containers
    variables = ["synaptor_param"]

    return worker_op(
               variables=variables, mount_point=MOUNT_PATH,
               task_id=f"generate_{taskname}", command=command,
               force_pull=True, on_failure_callback=task_failure_alert,
               image=SYNAPTOR_IMAGE, priority_weight=100_000,
               weight_rule=WeightRule.ABSOLUTE, queue=queue, dag=dag)


def worker_op(dag: DAG,
              i: int,
              op_queue_name: str = "worker",
              task_queue_name: str = TASK_QUEUE_NAME,
              ) -> Operator:
    """Runs a synaptor worker until it receives a kill task."""
    from airflow import configuration as conf
    broker_url = conf.get("celery", "broker_url")

    command = f"worker {broker_url} {task_queue_name}"

    # these variables will be mounted in the containers
    variables = ["synaptor_param"]

    return worker_op(
               variables=variables, mount_point=MOUNT_PATH,
               task_id=f"worker_{i}", command=command,
               force_pull=True, on_retry_callback=task_retry_alert,
               image=SYNAPTOR_IMAGE, priority_weight=100_000,
               weight_rule=WeightRule.ABSOLUTE, queue=op_queue_name, dag=dag)


def wait_op(dag: DAG, taskname: str) -> Operator:
    """Waits for a task to finish."""
    return PythonOperator(
               task_id=f"wait_for_queue_{taskname}",
               python_callable=check_queue,
               op_args=(TASK_QUEUE_NAME,),
               priority_weight=100_000,
               weight_rule=WeightRule.ABSOLUTE,
               queue="manager",
               dag=dag)


def drain_tasks_op() -> Operator:
    """Drains any tasks sitting in the queue. Skipping this for now"""
    pass


# DAG creation
generator_default_args = {
    "owner": "seuronbot",
    "depends_on_past": False,
    "start_date": datetime(2022, 2, 22),
    "catchup": False,
    "retries": 0,
}

dag_generator = DAG("synaptor_sanity_check",
                    default_args=generator_default_args,
                    schedule_interval=None)
#dag_worker = DAG("synaptor_worker",
#                 default_args=default_args,
#                 schedule_interval=None)


# Instantiating ops within DAGs
## Meta-stuff (cluster management & checks)
sanity_check = sanity_check_op(dag_generator, "manager")

check_image_labels = PythonOperator(
    task_id="check_image_labels",
    python_callable=check_image_labels_op,
    op_args=("inference_param",),
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_generator)

generate_ngl_link = PythonOperator(
    task_id="generate_ng_link",
    python_callable=generate_ngl_link_op,
    priority_weight=100_000,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_generator)

# scale_up_cluster = scale_up_cluster_op(
#         dag_worker, "synaptor", "cpu", 1, MAX_CLUSTER_SIZE, "cluster")
# scale_down_cluster = scale_down_cluster_op(
#         dag_worker, "synaptor", "cpu", 0, "cluster")

## Actual work ops
# workers = [synaptor_op(dag_worker, i) for i in range(MAX_CLUSTER_SIZE)]
# 
# generate_chunk_ccs = generate_tasks_op(dag_generator, "chunk_ccs")
# wait_chunk_ccs = wait_op(dag_generator, "chunk_ccs")
# 
# generate_merge_segs = generate_tasks_op(dag_generator, "merge_segs")
# wait_merge_segs = wait_op(dag_generator, "merge_segs")
# 
# generate_remap = generate_tasks_op(dag_generator, "remap")
# wait_remap = wait_op(dag_generator, "remap")
# 
# generate_kill = generate_tasks_op(dag_generator, "kill")


# Setting up dependencies
## Worker dag
#scale_up_cluster >> workers >> scale_down_cluster

## Generator/task dag - effectively making this a no-op dag for now
(sanity_check >> check_image_labels)  # >> drain_tasks  # skipping drain for now
# >> generate_chunk_ccs >> wait_chunk_ccs
# >> generate_merge_segs >> wait_merge_segs
# >> generate_remap >> wait_remap
# >> generate_kill >> generate_ngl_link)
