from seuronbot import SeuronBot
from airflow_api import set_variable, run_dag

from bot_info import broker_url
from bot_utils import download_file, download_json, clear_queues, replyto
import kombu_helper


@SeuronBot.on_message("update deepem parameters",
                      description="Update parameters for DeepEM training.",
                      cancelable=False,
                      file_inputs=True)
def update_deepem_params(msg):
    json_obj = download_json(msg)
    if json_obj:
        clear_queues()
        kombu_helper.drain_messages(broker_url, "deepem")
        kombu_helper.put_message(broker_url, "seuronbot_payload", json_obj)
        set_variable("deepem_param.json", json_obj, serialize_json=True)
        replyto(msg, "Update DeepEM parameters.")
    else:
        replyto(msg, "Error reading file.")


@SeuronBot.on_message("run deepem train",
                      description="Run DeepEM training.",
                      file_inputs=True)
def deepem_run(msg):
    _, payload = download_file(msg)
    if payload:
        set_variable("deepem_command", payload)
        replyto(msg, "Start DeepEM training.")
        run_dag("deepem_train")
