import os
import time
import threading
import logging
import base64
import fasteners
from slack_down import slack_md
import markdown

from IPython.core.magic import (Magics, magics_class, line_cell_magic)
from IPython.display import display, HTML, Code, Image

from kombu_helper import get_message, put_message, drain_messages


@magics_class
class SeuronBot(Magics):
    def __init__(self, shell):
        super(SeuronBot, self).__init__(shell)
        self.broker_url = "amqp://rabbitmq"
        self.output_queue = "jupyter-output-queue"
        self.input_queue = "jupyter-input-queue"
        self.slack_conv = markdown.Markdown(extensions=[slack_md.SlackExtension()])
        drain_messages(self.broker_url, self.input_queue)
        drain_messages(self.broker_url, self.output_queue)
        threading.Thread(target=self.forward_bot_message).start()

    @line_cell_magic
    def seuronbot(self, command, cell=None):
        msg_payload = {
                "text": command,
                "from_jupyter": True,
        }
        if cell:
            code_content = self.shell.input_transformer_manager.transform_cell(cell)
            msg_payload["attachment"] = base64.b64encode(code_content.encode("utf-8")).decode("utf-8")

        put_message(self.broker_url, self.input_queue, msg_payload)

    def ensure_dir(self, f):
        d = os.path.dirname(f)
        if d and not os.path.exists(d):
            os.makedirs(d)

    def forward_bot_message(self):
        while True:
            msg_payload = get_message(self.broker_url, self.output_queue, timeout=30)
            if msg_payload:
                display(HTML(self.slack_conv.convert(msg_payload.get("text", None))))
                attachment = msg_payload.get("attachment", None)
                if attachment:
                    if attachment["filetype"] in ["png", "jpeg", "gif"]:
                        display(Image(
                            data=base64.b64decode(attachment["content"]),
                            format=attachment["filetype"]
                        ))
                    elif attachment["filetype"] in ["python", "javascript"]:
                        display(Code(
                            data=base64.b64decode(attachment["content"]).decode("utf-8"),
                            language=attachment["filetype"]
                        ))
                time.sleep(1)


for k in logging.Logger.manager.loggerDict:
    logging.getLogger(k).setLevel(logging.CRITICAL)

lock = fasteners.InterProcessLock('/run/lock/seuronbot.lock')
if lock.acquire(timeout=10):
    lock_acquired = True
else:
    lock_acquired = False


def load_ipython_extension(ipython):
    if lock_acquired:
        seuronbot = SeuronBot(ipython)
        ipython.register_magics(seuronbot)
        print("'seuronbot' magic loaded.")
        print('Use "%seuronbot help" to list all available commands')
        print('Use cell magic "%%seuronbot" for commands requiring additinoal cell input')
    else:
        print("Another seuronbot is already running")
