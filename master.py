from flask import Flask, request, render_template_string
from mpi4py import MPI
import time
import logging
import boto3
import threading
import json
import os
import signal

logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/696726802797/TaskQueueForCrawlers'

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

seed_tasks = [
    {"url": "http://example.org", "allowed_domains": ["example.org"], "obey_robots": True, "delay": 1.0, "depth": 1}
]

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

# Flask app for handling web client requests
app = Flask(__name__)
search_results = []

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
            seed_tasks.append(task)
            return render_template_string(open("templates/index.html").read(), results="URL task added successfully.")
        elif command_type == "search":
            keyword = request.form["query"].strip()
            comm.send(keyword, dest=2, tag=10)
            result = comm.recv(source=2, tag=11)
            return render_template_string(open("templates/index.html").read(), results=f"Search results for '{keyword}': {result}")
    return render_template_string(open("templates/index.html").read(), results=None)

def send_seed_tasks_to_queue(tasks):
    message_ids = []
    for task in tasks:
        message_id = send_to_queue(task)
        if message_id:
            message_ids.append(message_id)
    return message_ids

def start_flask_server():
    app.run(host='0.0.0.0', port=5000, debug=False)

stop_event = threading.Event()

def ping_thread_func(comm, status, crawler_tasks, active_crawlers, num_tasks, task_dict, c, crawler_tasks_assigned):
    while not stop_event.is_set():
        try:
            comm.send("ping", dest=1, tag=100)
            logging.info("Ping sent to crawler 1")
            crawlers_that_responded = []
            time.sleep(0.5)

            if comm.iprobe(source=1, tag=101, status=status):
                comm.recv(source=1, tag=101)
                c[0] += 1
                crawlers_that_responded.append(1)
                logging.info(f"Pong received from crawler 1")
            else:
                logging.warning("Crawler 1 unresponsive")
                if crawler_tasks[1] != "def":
                    failed_task_id = crawler_tasks[1]
                    failed_task = task_dict.get(failed_task_id)
                    if failed_task:
                        send_seed_tasks_to_queue([failed_task])
                        num_tasks[0] += 1
                        del task_dict[failed_task_id]
                    crawler_tasks_assigned[0] -= 1
                    crawler_tasks[1] = "def"
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error in ping thread: {e}")

def master_process():
    status = MPI.Status()
    logging.info(f"Master node started with rank {rank}")

    size = comm.Get_size()
    if size < 3:
        logging.error("At least 3 MPI processes required (1 master, 1 crawler, 1 indexer)")
        return

    crawler_nodes = size - 2
    indexer_rank = crawler_nodes + 1
    active_crawlers = list(range(1, crawler_nodes + 1))

    keywords = ["hello", "this", "python"]

    message_ids = send_seed_tasks_to_queue(seed_tasks)
    num_tasks = [len(message_ids)]
    task_dict = dict(zip(message_ids, seed_tasks))
    task_dict["def"] = "placeholder"

    crawler_tasks_assigned = [0]
    crawler_tasks = ["def"] * (crawler_nodes + 1)
    indexer_is_available = True
    remaining_keywords = keywords
    num_keywords = len(keywords)
    discovered_urls = []
    c = [0]

    # Start ping thread
    ping_thread = threading.Thread(target=ping_thread_func, args=(
        comm, status, crawler_tasks, active_crawlers, num_tasks, task_dict, c, crawler_tasks_assigned
    ))
    ping_thread.start()

    while True:
        # Assign tasks to crawlers
        for crawler_rank in list(active_crawlers):
            if num_tasks[0] > 0:
                comm.send("start", dest=crawler_rank, tag=42)
                crawler_tasks_assigned[0] += 1
                num_tasks[0] -= 1
                active_crawlers.remove(crawler_rank)
                logging.info(f"Sent task to crawler {crawler_rank}")

        # Send keyword to indexer if available
        if num_keywords > 0 and indexer_is_available:
            comm.send(remaining_keywords[0], dest=indexer_rank, tag=10)
            indexer_is_available = False
            num_keywords -= 1
            remaining_keywords = remaining_keywords[1:]

        # Handle incoming messages
        while comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            source = status.Get_source()
            tag = status.Get_tag()

            if tag != 101:
                message_data = comm.recv(source=source, tag=tag)

            if tag == 1:
                crawler_tasks_assigned[0] -= 1
                active_crawlers.append(source)
                completed_task_id = message_data.get("task_id")
                crawler_tasks[source] = "def"
                logging.info(f"Crawler {source} finished task {completed_task_id}")

            elif tag == 2:
                crawler_tasks[source] = message_data

            elif tag == 11:
                indexer_is_available = True
                logging.info(f"Indexer {source} returned result: {message_data}")

            elif tag == 99:
                discovered_urls.extend(message_data)

            elif tag == 999:
                crawler_tasks_assigned[0] -= 1
                logging.error(f"Crawler {source} reported error: {message_data}")

        # Check if Flask added new tasks
        if len(seed_tasks) > len(task_dict) - 1:  # subtract 1 for "def"
            new_tasks = seed_tasks[len(task_dict) - 1:]
            new_ids = send_seed_tasks_to_queue(new_tasks)
            for i, msg_id in enumerate(new_ids):
                task_dict[msg_id] = new_tasks[i]
            num_tasks[0] += len(new_ids)

        time.sleep(0.1)

    stop_event.set()
    ping_thread.join()

# Start Flask app in a thread
if rank == 0:
    flask_thread = threading.Thread(target=start_flask_server)
    flask_thread.daemon = True
    flask_thread.start()
    master_process()
