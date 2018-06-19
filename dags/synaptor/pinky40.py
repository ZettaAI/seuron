from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_plugin import DockerWithVariablesOperator

DAG_ID = 'synaptor_pinky40'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'cactchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None
)

# =============
# run-specific args
img_cvname = "gs://neuroglancer/pinky40_v11/image"
seg_cvname = "gs://neuroglancer/pinky40_v11/watershed_mst_trimmed_sem_remap"
out_cvname = "gs://neuroglancer/pinky40_v11/psd-w4_wt_focused_760k"
cleft_cvname = "gs://neuroglancer/pinky40_v11/clefts"
wshed_cvname = "gs://neuroglancer/pinky40_v11/watershed"

# FULL VOLUME COORDS
start_coord = (10240,4096,0)
vol_shape   = (57344,40960,1024)
chunk_shape = (1024,1024,1024)

# TEST VOLUME COORDS
start_coord = (38912, 24576, 512)
vol_shape = (2048, 2048, 256)
chunk_shape = (1024, 1024, 128)

cc_thresh = 0.25
sz_thresh1 = 100
sz_thresh2 = 500
cc_dil_param = 0

num_samples = 2
asyn_dil_param = 5
patch_sz = (160, 160, 18)

voxel_res = (4, 4, 40)
dist_thr = 1000

proc_dir_path = "gs://seunglab/nick/pinky40"
# =============

import itertools
import operator


def chunk_bboxes(vol_size, chunk_size, offset=None):

    x_bnds = bounds1D(vol_size[0], chunk_size[0])
    y_bnds = bounds1D(vol_size[1], chunk_size[1])
    z_bnds = bounds1D(vol_size[2], chunk_size[2])

    bboxes = [tuple(zip(xs, ys, zs))
              for (xs, ys, zs) in itertools.product(x_bnds, y_bnds, z_bnds)]

    if offset is not None:
        bboxes = [(tuple(map(operator.add, bb[0], offset)),
                   tuple(map(operator.add, bb[1], offset)))
                  for bb in bboxes]

    return bboxes


def bounds1D(full_width, step_size):

    assert step_size > 0, "invalid step_size: {}".format(step_size)
    assert full_width > 0, "invalid volume_width: {}".format(full_width)

    start = 0
    end = step_size

    bounds = []
    while end < full_width:
        bounds.append((start, end))

        start += step_size
        end += step_size

    # last window
    bounds.append((start, end))

    return bounds
# =============


def chunk_ccs(dag, chunk_begin, chunk_end):

    chunk_begin_str = " ".join(map(str,chunk_begin))
    chunk_end_str = " ".join(map(str,chunk_end))

    return DockerWithVariablesOperator(
        ["project_name","google-secret.json","aws-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="chunk_ccs_" + "_".join(map(str,chunk_begin)),
        command=("chunk_ccs {out_cvname} {cleft_cvname} {proc_dir_path} " +
                 "{cc_thresh} {sz_thresh}" +
                 "--chunk_begin {chunk_begin_str} " +
                 "--chunk_end {chunk_end_str}"
                 ).format(out_cvname=out_cvname, cleft_cvname=cleft_cvname,
                          proc_dir_path=proc_dir_path, cc_thresh=cc_thresh,
                          sz_thresh=sz_thresh1, cc_dil_param=cc_dil_param,
                          chunk_begin_str=chunk_begin_str,
                          chunk_end_str=chunk_end_str),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        #queue="cpu",
        dag=dag
        )


def merge_ccs(dag):
    return DockerWithVariablesOperator(
        ["project_name","google-secret.json","aws-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="merge_ccs",
        command=("merge_ccs {proc_dir_path} {sz_thresh}"
                      ).format(proc_dir_path=proc_dir_path, sz_thresh=sz_thresh1),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        #queue="cpu",
        dag=dag
        )


def asynet_pass(dag, chunk_begin, chunk_end):

    chunk_begin_str = " ".join(map(str,chunk_begin))
    chunk_end_str = " ".join(map(str,chunk_end))
    patchsz_str = " ".join(map(str,patch_sz))

    return DockerWithVariablesOperator(
        ["project_name","google-secret.json","aws-secret.json"],
        host_args={"runtime": "nvidia"},
        mount_point="/root/.cloudvolume/secrets",
        task_id="chunk_edges" + "_".join(map(str,chunk_begin)),
        command=("chunk_edges {img_cvname} {cleft_cvname} {seg_cvname} " +
                      "{proc_dir_path} {num_samples} {dil_param} " +
                      "--chunk_begin {chunk_begin_str} " +
                      "--chunk_end {chunk_end_str} " +
                      "--wshed_cvname {wshed_cvname} " +
                      "--patchsz {patchsz_str}"
                      ).format(img_cvname=img_cvname, cleft_cvname=cleft_cvname,
                               seg_cvname=seg_cvname, wshed_cvname=wshed_cvname,
                               num_samples=num_samples,
                               dil_param=asyn_dil_param,
                               chunk_begin_str=chunk_begin_str,
                               chunk_end_str=chunk_end_str,
                               patchsz_str=patchsz_str,
                               proc_dir_path=proc_dir_path),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        #queue="gpu",
        dag=dag
        )


def merge_edges(dag):
    voxel_res_str = " ".join(map(str,voxel_res))
    return DockerWithVariablesOperator(
        ["project_name","google-secret.json","aws-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="merge_edges",
        command=("merge_edges {proc_dir_path} {dist_thr} {size_thr}" +
                      "--voxel_res {voxel_res_str} "
                      ).format(proc_dir_path=proc_dir_path, dist_thr=dist_thr,
                               size_thr=size_thr2, voxel_res_str=voxel_res_str),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        #queue="cpu",
        dag=dag
        )


def remap_ids(dag, chunk_begin, chunk_end):
    chunk_begin_str = " ".join(map(str,chunk_begin))
    chunk_end_str = " ".join(map(str,chunk_end))
    return DockerWithVariablesOperator(
        ["project_name","google-secret.json","aws-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="remap_ids" + "_".join(map(str,chunk_begin)),
        command=("remap_ids {cleft_cvname} {cleft_cvname} {proc_dir_path} " +
                      "--chunk_begin {chunk_begin_str} " +
                      "--chunk_end {chunk_end_str}"
                      ).format(cleft_cvname=cleft_cvname,
                               proc_dir_path=proc_dir_path,
                               chunk_begin_str=chunk_begin_str,
                               chunk_end_str=chunk_end_str),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        #queue="cpu",
        dag=dag
        )


# STEP 1: chunk_ccs
bboxes = chunk_bboxes(vol_shape, chunk_shape, offset=start_coord)
step1 = [chunk_ccs(dag, bb[0], bb[1]) for bb in bboxes]

# STEP 2: merge_ccs
step2 = merge_ccs(dag)
for chunk in step1:
    chunk.set_downstream(step2)

# STEP 3: asynet pass
step3 = [asynet_pass(dag, bb[0], bb[1]) for bb in bboxes]
for chunk in step3:
    step2.set_downstream(chunk)

# STEP 4: merge edges
step4 = merge_edges(dag)
for chunk in step3:
    chunk.set_downstream(step4)

# STEP 5: remap_ids
step5 = [remap_ids(dag, bb[0], bb[1]) for bb in bboxes]
for chunk in step5:
    step4.set_downstream(chunk)
