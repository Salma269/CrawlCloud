from flask import Flask, request, render_template_string
from collections import defaultdict
from mpi4py import MPI
import time
import logging
import boto3
import threading
import json
import os
import sys
import signal

logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/696726802797/TaskQueueForCrawlers'

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
status = MPI.Status()

app = Flask(__name__)
search_results = []
seed_tasks = []

task_dict = {}
num_tasks = [0]
crawler_tasks_assigned = [0]
crawler_tasks = []
c = [0]

active_crawlers = list(range(1, size - 1))
indexer_rank = size - 1

stop_event = threading.Event()


def send_to_queue(message):
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )
        message_id = response['MessageId']
        logging.info(f"Message sent to SQS, Message ID: {message_id}")
        return message_id
    except Exception as e:
        logging.error(f"Failed to send message to SQS: {e}")
        return None


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        command_type = request.form["command_type"]
        if command_type == "url":
            url = request.form["url"].strip()
            domain = request.form["domain"].strip()
            depth = int(request.form["depth"])
            task = {
                "url": url,
                "allowed_domains": [domain],
                "obey_robots": True,
                "delay": 1.0,
                "depth": depth
            }
            message_id = send_to_queue(task)
            if message_id:
                task_dict[message_id] = task
                num_tasks[0] += 1
            return render_template_string(open("templates/index.html").read(), results="URL task added successfully.")
        elif command_type == "search":
            keyword = request.form["query"].strip()
            comm.send(keyword, dest=indexer_rank, tag=10)
            result = comm.recv(source=indexer_rank, tag=11)
            return render_template_string(open("templates/index.html").read(), results=f"Search results for '{keyword}': {result}")
    return render_template_string(open("templates/index.html").read(), results=None)


def start_flask_server():
    app.run(host='0.0.0.0', port=5000, debug=False)


def send_seed_tasks_to_queue(seed_tasks):
    message_ids = []
    for task in seed_tasks:
        message_id = send_to_queue(task)
        if message_id:
            message_ids.append(message_id)
    return message_ids


def ping_thread_func(comm, status, crawler_tasks, active_crawlers, num_tasks, task_dict, c, crawler_tasks_assigned, indexer_rank):
    while not stop_event.is_set():
        try:
            for crawler_id in active_crawlers + [i for i in range(1, len(crawler_tasks)) if crawler_tasks[i] != "def"]:
                comm.send("ping", dest=crawler_id, tag=100)
                logging.info(f"Ping sent to crawler {crawler_id}")

            comm.send("ping", dest=indexer_rank, tag=100)
            logging.info(f"Ping sent to indexer {indexer_rank}")

            crawlers_that_responded = []
            indexer_responded = False

            time.sleep(0.5)

            while comm.iprobe(source=MPI.ANY_SOURCE, tag=101, status=status):
                source = status.Get_source()
                comm.recv(source=source, tag=101)
                if source == indexer_rank:
                    indexer_responded = True
                    logging.info(f"Pong received from indexer {source}")
                else:
                    c[0] += 1
                    crawlers_that_responded.append(source)
                    logging.info(f"Pong received from crawler {source}")

            for crawler_id in range(1, len(crawler_tasks)):
                if crawler_id not in crawlers_that_responded and crawler_tasks[crawler_id] != "def":
                    logging.warning(f"Crawler {crawler_id} did not respond")
                    failed_task_id = crawler_tasks[crawler_id]
                    failed_task = task_dict.get(failed_task_id)
                    if failed_task:
                        send_to_queue(failed_task)
                        num_tasks[0] += 1
                        del task_dict[failed_task_id]
                    crawler_tasks_assigned[0] -= 1
                    crawler_tasks[crawler_id] = "def"

            if not indexer_responded:
                logging.warning(f"Indexer {indexer_rank} did not respond")

        except Exception as e:
            logging.error(f"Error in ping thread: {e}")

        time.sleep(1)


def master_process():
    global crawler_tasks
    logging.info(f"Master node started with rank {rank} of {size}")

    if size < 3:
        logging.error("Need at least 3 nodes: 1 master, 1 crawler, 1 indexer.")
        return

    crawler_tasks = ["def"] * size

    ping_thread = threading.Thread(target=ping_thread_func, args=(
        comm, status, crawler_tasks, active_crawlers, num_tasks, task_dict, c, crawler_tasks_assigned, indexer_rank
    ))
    ping_thread.start()

    while True:
        for crawler_rank in active_crawlers:
            if num_tasks[0] > 0:
                comm.send("start", dest=crawler_rank, tag=42)
                message_id, task = next(((tid, t) for tid, t in task_dict.items() if tid != "def"), (None, None))
                if message_id:
                    comm.send(task, dest=crawler_rank, tag=2)
                    crawler_tasks[crawler_rank] = message_id
                    del task_dict[message_id]
                    num_tasks[0] -= 1
                    crawler_tasks_assigned[0] += 1
                    active_crawlers.remove(crawler_rank)
                    logging.info(f"Sent task to crawler {crawler_rank}")

        while comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            source = status.Get_source()
            tag = status.Get_tag()

            if tag != 101:
                message = comm.recv(source=source, tag=tag)

            if tag == 1:
                logging.info(f"Received DONE from crawler {source}")
                crawler_tasks_assigned[0] -= 1
                crawler_tasks[source] = "def"
                active_crawlers.append(source)

            elif tag == 2:
                crawler_tasks[source] = message
                logging.info(f"Crawler {source} reported task start")

            elif tag == 11:
                logging.info(f"Received search result from indexer: {message}")

            elif tag == 99:
                logging.info(f"Received URLs from crawler {source}")

            elif tag == 999:
                logging.error(f"Crawler {source} reported error: {message}")
                crawler_tasks_assigned[0] -= 1
                crawler_tasks[source] = "def"

        time.sleep(0.1)


flask_thread = threading.Thread(target=start_flask_server)
flask_thread.start()

if __name__ == '__main__':
    master_process()
