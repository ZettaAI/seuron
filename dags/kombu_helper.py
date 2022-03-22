from kombu import Connection
from kombu.simple import SimpleQueue
from time import sleep
import requests


def check_queue(queue_name):
    ret = requests.get(
        f"http://rabbitmq:15672/api/queues/%2f/{queue_name}", auth=("guest", "guest")
    )
    if not ret.ok:
        raise RuntimeError("Cannot connect to rabbitmq management interface")
    queue_status = ret.json()
    return queue_status


# Can't use the variable name "conn" as an argument bc it's reserved by airflow
def drain_messages(connstr, queue_name):
    with Connection(connstr) as conn:
        simple_queue = conn.SimpleQueue(queue_name)
        num_drained = 0
        while True:
            try:
                message = simple_queue.get_nowait()
            except SimpleQueue.Empty:
                status = check_queue(queue_name)
                if "messages" not in status:
                    sleep(30)
                    continue
                elif status["messages"] > 0:
                    print(
                        f'still {status["messages"]} messages left in the queue, sleep for 30s'
                    )
                    sleep(30)
                    continue
                else:
                    break
            # print(f'message: {message.payload}')
            message.ack()
            num_drained += 1

        simple_queue.close()
        print(f"# tasks drained: {num_drained}")
