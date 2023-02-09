"""Operator functions for DeepEM DAGs."""
from __future__ import annotations
import io
import re
from typing import Optional

from airflow import DAG
from airflow.utils.weight_rule import WeightRule
from airflow.models import Variable, BaseOperator as Operator

from worker_op import worker_op


# Hard-code these for now
MOUNT_POINT = "/root/.cloudvolume/secrets/"
DEEPEM_IMAGE = "gcr.io/zetta-lee-fly-vnc-001/deepem:expt0"
TASK_QUEUE_NAME = "deepem"


def deepem_op(
    dag: DAG,
    worker_id: int,
    queue: Optional[str] = "deepem-gpu"
) -> Operator:
    """Run a DeepEM worker."""
    variables = add_secrets_if_defined([])

    command = Variable.get("deepem_command")
    command = " ".join(parsecmd(command))

    return worker_op(
        variables=variables,
        mount_point=MOUNT_POINT,
        task_id=f"worker_{worker_id}",
        command=command,
        force_pull=True,
        image=DEEPEM_IMAGE,
        priority_weight=100_000,
        weight_rule=WeightRule.ABSOLUTE,
        queue=queue,
        dag=dag
    )


# Helper functions
def parsecmd(cmdstr: str) -> list[str]:
    """Parses a command string.

    Adapted from
        https://github.com/ZettaAI/samwise/blob/main/samwise/parse.py#L7

    Args:
        cmdstr: Command string
    Returns:
        A list of shell arguments
    """
    with io.StringIO(cmdstr) as f:
        lines = f.readlines()
        cleaned = [re.sub(r"\\\n|\n|\\", "", line) for line in lines]

    args = list()
    for line in cleaned:
        for arg in line.split():
            if arg != "":
                args.append(arg)

    return args


def add_secrets_if_defined(variables: list[str]) -> list[str]:
    """Adds CloudVolume secret files to the mounted variables if defined."""
    maybe_gcp = Variable.get("google-secret.json", None)

    if maybe_gcp is not None:
        variables.append("google-secret.json")

    return variables
