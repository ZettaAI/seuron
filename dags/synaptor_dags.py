"""DAG definition for synaptor workflows."""
from datetime import datetime
from configparser import ConfigParser

from airflow import DAG
from airflow.models import Variable

# from helper_ops import mark_done_op, slack_message_op
from helper_ops import scale_up_cluster_op, scale_down_cluster_op
from param_default import default_synaptor_param
from synaptor_ops import manager_op, drain_op
from synaptor_ops import synaptor_op, wait_op, generate_op


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


default_args = {
    "owner": "seuronbot",
    "depends_on_past": False,
    "start_date": datetime(2022, 2, 22),
    "catchup": False,
    "retries": 0,
}

# =========================================
# Sanity check DAG
# "update synaptor params"

dag_sanity = DAG(
    "synaptor_sanity_check",
    default_args=default_args,
    schedule_interval=None,
    tags=["synaptor"],
)

manager_op(dag_sanity, "sanity_check")


# =========================================
# File segmentation
# "run synaptor file segmentation"

fileseg_dag = DAG(
    "synaptor_file_seg",
    default_args=default_args,
    schedule_interval=None,
    tags=["synaptor"],
)


# OP INSTANTIATION
# Cluster management
drain = drain_op(fileseg_dag)
sanity_check = manager_op(fileseg_dag, "sanity_check")
init_cloudvols = manager_op(fileseg_dag, "init_cloudvols")

scale_up_cluster = scale_up_cluster_op(
    fileseg_dag, "synaptor", "synaptor-cpu", 1, MAX_CLUSTER_SIZE, "cluster"
)
scale_down_cluster = scale_down_cluster_op(
    fileseg_dag, "synaptor", "synaptor-cpu", 0, "cluster"
)


# Ops that do actual work
workers = [synaptor_op(fileseg_dag, i) for i in range(MAX_CLUSTER_SIZE)]

generate_chunk_ccs = generate_op(fileseg_dag, "chunk_ccs")
wait_chunk_ccs = wait_op(fileseg_dag, "chunk_ccs")

generate_merge_ccs = generate_op(fileseg_dag, "merge_ccs")
wait_merge_ccs = wait_op(fileseg_dag, "merge_ccs")

generate_remap = generate_op(fileseg_dag, "remap")
wait_remap = wait_op(fileseg_dag, "remap")

generate_self_destruct = generate_op(fileseg_dag, "self_destruct")


# DEPENDENCIES
# Drain old tasks before doing anything
drain >> sanity_check >> init_cloudvols

# Worker dag
(init_cloudvols >> scale_up_cluster >> workers >> scale_down_cluster)

# Generator/task dag
(
    init_cloudvols
    # >> sanity_check_report
    >> generate_chunk_ccs
    >> wait_chunk_ccs
    >> generate_merge_ccs
    >> wait_merge_ccs
    >> generate_remap
    >> wait_remap
    >> generate_self_destruct
    # >> generate_ngl_link
)


# =========================================
# DB segmentation
# "run synaptor db segmentation"

dbseg_dag = DAG(
    "synaptor_db_seg",
    default_args=default_args,
    schedule_interval=None,
    tags=["synaptor"],
)


# OP INSTANTIATION
# Cluster management
drain = drain_op(dbseg_dag)
sanity_check = manager_op(dbseg_dag, "sanity_check")
init_cloudvols = manager_op(dbseg_dag, "init_cloudvols")
init_db = manager_op(dbseg_dag, "init_db")

scale_up_cluster = scale_up_cluster_op(
    dbseg_dag, "synaptor", "synaptor-cpu", 1, MAX_CLUSTER_SIZE, "cluster"
)
scale_down_cluster = scale_down_cluster_op(
    dbseg_dag, "synaptor", "synaptor-cpu", 0, "cluster"
)


# Ops that do actual work
workers = [synaptor_op(dbseg_dag, i) for i in range(MAX_CLUSTER_SIZE)]

generate_chunk_ccs = generate_op(dbseg_dag, "chunk_ccs")
wait_chunk_ccs = wait_op(dbseg_dag, "chunk_ccs")

generate_match_contins = generate_op(dbseg_dag, "match_contins")
wait_match_contins = wait_op(dbseg_dag, "match_contins")

generate_seg_graph_ccs = generate_op(dbseg_dag, "seg_graph_ccs")
wait_seg_graph_ccs = wait_op(dbseg_dag, "seg_graph_ccs")

generate_chunk_seg_map = generate_op(dbseg_dag, "chunk_seg_map")
wait_chunk_seg_map = wait_op(dbseg_dag, "chunk_seg_map")

generate_merge_seginfo = generate_op(dbseg_dag, "merge_seginfo")
wait_merge_seginfo = wait_op(dbseg_dag, "merge_seginfo")

generate_remap = generate_op(dbseg_dag, "remap")
wait_remap = wait_op(dbseg_dag, "remap")

generate_self_destruct = generate_op(dbseg_dag, "self_destruct")


# DEPENDENCIES
# Drain old tasks before doing anything
drain >> sanity_check >> init_cloudvols >> init_db

# Worker dag
(init_db >> scale_up_cluster >> workers >> scale_down_cluster)

# Generator/task dag
(
    init_db
    # >> sanity_check_report
    >> generate_chunk_ccs
    >> wait_chunk_ccs
    >> generate_match_contins
    >> wait_match_contins
    >> generate_seg_graph_ccs
    >> wait_seg_graph_ccs
    >> generate_chunk_seg_map
    >> wait_chunk_seg_map
    >> generate_merge_seginfo
    >> wait_merge_seginfo
    >> generate_remap
    >> wait_remap
    >> generate_self_destruct
    # >> generate_ngl_link
)
