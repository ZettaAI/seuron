"""DAG definitions for DeepEM workflows."""
from datetime import datetime

from airflow import DAG

from helper_ops import (
    scale_up_cluster_op,
    scale_down_cluster_op,
    collect_metrics_op
)
from deepem_ops import deepem_op


default_args = {
    "owner": "seuronbot",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
    "retries": 0,
}


train_dag = DAG(
    "deepem_train",
    default_args=default_args,
    schedule_interval=None,
    tags=["deepem"],
)


# OP INSTANTIATION
scale_up_cluster = scale_up_cluster_op(
    train_dag, "deepem", "deepem-gpu", 1, 1, "cluster"
)
scale_down_cluster = scale_down_cluster_op(
    train_dag, "deepem", "deepem-gpu", 0, "cluster"
)


# DEPENDENCIES
(
    collect_metrics_op(train_dag)
    >> scale_up_cluster
    >> deepem_op(train_dag, 0)
    >> scale_down_cluster
)
