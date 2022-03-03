"""DAG definition for synaptor basic segmentation workflow (file-backend).

The entire DAG consists of two sub dags (similar to chunkflow_dag.py):
* dag_generator specifies the work for a node that generates tasks and monitors
their completion through the RabbitMQ
* dag_worker specifies how to manage the cluster of worker nodes. This becomes
  "scale up, wait until the workflow is finished, report results, and scale
  down".
"""
import json

from airflow import DAG
from airflow.models import Variable
from airflow.utils.weight_rule import WeightRule

from worker_op import worker_op
from igneous_and_cloudvolume import check_queue
from helper_ops import mark_done_op, slack_message_op
from helper_ops import scale_up_cluster_op, scale_down_cluster_op
from param_default import synaptor_param_default, check_worker_image_labels
from param_default import default_mount_path


# Processing parameters
# need to set a default so any airflow container can parse the dag (?)
Variable.setdefault("synaptor_param", synaptor_param_default,
                    deserialize_json=True)
param = Variable.get("synaptor_param", deserialize_json=True)


# Python callables (for PythonOperators)
def sanity_check(dag, param):
    """Checks the uploaded processing parameters for problems."""
    pass


# Op functions
def worker_op(dag):
    """Runs a worker until all tasks are complete."""
    pass


def drain_tasks_op():
    """Drains any tasks sitting in the queue."""
    from airflow import configuration as conf
    broker_url = conf.get("celery", "broker_url")

    command = f'bash -c "drain_tasks {broker_url}"'

    variables = ["synaptor_param"]
    if "MOUNT_SECRETS" in param:
        variables += param["MOUNT_SECRETS"]

    mount_point = param.get("MOUNT_PATH", default_mount_path)
    return worker_op(
               variables=variables, mount_point=mount_point,
               task_id='drain_tasks', command=command,
               force_pull=True, on_failure_callback=task_failure_alert,
               image=param["SYNAPTOR_IMAGE"], priority_weight=100_000,
               weight_rule=WeightRule.ABSOLUTE, queue=queue, dag=dag)


def generate_tasks_op(dag, taskname):
    """Generates tasks to run and adds them to the RabbitMQ."""
    pass


# Instantiating ops
sanity_check = PythonOperator(
    task_id="sanity_check",
    python_callable=sanity_check_op,
    op_args = tuple(),
    on_failure_callback=task_failure_alert,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_generator)

scale_up_cluster = scale_up_cluster_op(
        dag_worker, "synaptor", "cpu",
        min(param.get("TASK_NUM", 1), 20),
        min(param.get("TASK_NUM", 1), 20),  # not sure what these args mean yet
        "cluster")
scale_down_cluster = scale_down_cluster_op(
        dag_worker, "synaptor", "cpu", 0, "cluster")

wait_for_queue = PythonOperator(
    task_id="wait_for_queue",
    python_callable=check_queue,
    op_args=("synaptor",),
    priority_weight=100_000,
    weight_rule=WeightRule.ABSOLUTE,
    queue="manager",
    dag=dag_worker
)

generate_chunk_ccs = generate_tasks_op(dag_worker, "chunk_ccs")
generate_chunk_seg_map = generate_tasks_op(dag_worker, "merge_segs")
generate_remap= generate_tasks_op(dag_worker, "remap")

workers = [worker_op(dag, i) for i in range(param.get("TASK_NUM", 1))]


# DAG definition
## Worker dag
scale_up_cluster >> workers >> scale_down_cluster

(scale_up_cluster >> wait_for_synaptor
 >> generate_ng_link >> scale_down_cluster)

## Task dag
(sanity_check >> image_parameters >> drain_tasks
 >> generate_chunk_ccs >> wait_for_queue
 >> generate_merge_segs >> wait_for_queue
 >> generate_remap >> wait_for_queue
 >> mark_done)
