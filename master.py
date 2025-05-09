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
seed_tasks = [
    #{"url": "http://quotes.toscrape.com/", "allowed_domains": ["toscrape.com"], "obey_robots": True, "delay": 1.0, "depth": 1},
    #{"url": "http://quotes.toscrape.com/", "allowed_domains": ["toscrape.com"], "obey_robots": True, "delay": 1.0, "depth": 1},
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


def foo():
    sys.exit(0)

# Flask app for handling web client requests
app = Flask(__name__)
search_results = []  # Define this at the global/module level

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
            print(seed_tasks)  # Debug print
            return render_template_string(open("templates/index.html").read(), results="URL task added successfully.")

        elif command_type == "search":
            keyword = request.form["query"].strip()
            comm.send(keyword, dest=2, tag=10)
            result = comm.recv(source=2, tag=11)
            return render_template_string(open("templates/index.html").read(), results=f"Search results for '{keyword}': {result}")

    return render_template_string(open("templates/index.html").read(), results=None)


def send_seed_tasks_to_queue(seed_tasks):
    message_ids = []
    for task in seed_tasks:
        message_id = send_to_queue(task)
        if message_id:
            message_ids.append(message_id)
        else:
            logging.error("Failed to send task to SQS")
    
    return message_ids


# Function to start Flask web server in a separate thread
def start_flask_server():
    app.run(host='0.0.0.0', port=5000, debug=False)

ping_lock = threading.Lock()
stop_event = threading.Event()

def ping_thread_func(comm, ping_interval, status, crawler_tasks, active_crawlers, num_tasks, task_dict, c, crawler_tasks_assigned):
    # global inactive_crawlers
    while not stop_event.is_set():
        # with ping_lock:
        #     inactive_crawlers
        logging.info("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        crawlers_that_responded = []

        # Wait and listen for pong replies for `ping_interval` seconds
        ping_deadline = time.time() + 5
        logging.info("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        while time.time() < ping_deadline:
            logging.info("cccccccccccccccccccccccccccccccccccccccccccccc")
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=101, status=status):
                logging.info("ddddddddddddddddddddddddddddddddddddddddddddddddd")
                source = status.Get_source()
                comm.recv(source=source, tag=101)
                logging.info("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
                c[0] += 1
                logging.info(f"{c[0]} Received pong from Crawler {source}")

                if source not in crawlers_that_responded:
                    crawlers_that_responded.append(source)
                else:
                    logging.info("fffffffffffffffffffffffffffffffffffffffffffffffff")
                    time.sleep(0.05)

        logging.info("gggggggggggggggggggggggggggggggggggggggggggggggg")

        # unresponsive = [c for c in active_crawlers if c not in crawlers_that_responded]
        # for crawler in unresponsive:
        if 1 in crawlers_that_responded:
            logging.info(f"Crawler responded")
            crawlers_that_responded.remove(1)
        else:
            if crawler_tasks[1] != "def":
                logging.error(f"Crawler did not respond to ping.")
                foo()
                num_tasks[0] += 1
                # logging.warning(f"Task ID {crawler_tasks[1]} not found in task_dict")
                # logging.warning(f"Task dict {task_dict}")
                send_seed_tasks_to_queue([task_dict[crawler_tasks[1]]])
                logging.info(f"Reuploaded task from failed crawler 1")
                crawler_tasks_assigned[0] -= 1
                # active_crawlers.append(1)
                del task_dict[crawler_tasks[1]]
                crawler_tasks[1] = "def"
                
        # if(c[0] < 4): #simulate fault tolerance
        comm.send("ping", dest=1, tag=100)
        logging.info(f"Sent ping to Crawler 1")
        time.sleep(0.1)  # Slight delay before next ping loop


def master_process():
    # comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()

    logging.info(f"Master node started with rank {rank} of {size}")

    crawler_nodes = size - 2
    indexer_nodes = 1
    if crawler_nodes <= 0 or indexer_nodes <= 0:
        logging.error("Need at least 3 nodes: 1 master, 1 crawler, 1 indexer.")
        return
    
    active_indexers = crawler_nodes + 1  

    keywords = ["hello", "this", "python"]

    remaining_tasks = send_seed_tasks_to_queue(seed_tasks)
    num_tasks = [len(remaining_tasks)]
    task_dict = dict(zip(remaining_tasks, seed_tasks))
    task_dict["def"] = "yarab"

    remaining_keywords = keywords
    num_keywords = 0

    crawler_tasks_assigned = [0]
    # indexer_tasks_assigned = 0
    indexer_is_available = True
    discovered_urls = []

    crawler_tasks = ["def"] * (crawler_nodes + 1)

    c = [0]
    ping_thread = threading.Thread(target=ping_thread_func, args=(comm, 2, status, crawler_tasks, active_crawlers, num_tasks, task_dict, c, crawler_tasks_assigned))
    ping_thread.start()

    while crawler_tasks_assigned[0] > 0 or num_tasks[0] > 0:

        # logging.info(f"{task_dict[crawler_tasks[1]]} -> {crawler_tasks[1]} -> {crawler_tasks}")
        for crawler_rank in active_crawlers:
            if num_tasks[0] > 0:
                comm.send("start", dest=crawler_rank, tag=42)
                crawler_tasks_assigned[0] += 1
                num_tasks[0] -= 1
                active_crawlers.remove(crawler_rank)
                logging.info(f"Sent start signal to Crawler {crawler_rank}, -- {crawler_tasks_assigned[0]} -- {num_tasks[0]} ")

        # for indexer_rank in active_indexers:
        if num_keywords > 0:
            if indexer_is_available:
                comm.send(remaining_keywords[0], dest=2, tag=10)
                # remaining_keywords = remaining_keywords[1:]
                # indexer_tasks_assigned += 1
                # num_keywords -= 1
                # active_indexers.remove(indexer_rank)
                logging.info(f"Sent start signal to Indexer {2} ")
                indexer_is_available = False

        while comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            source = status.Get_source()
            tag = status.Get_tag()
            message_data = comm.recv(source=source, tag=tag)
            if tag == 1:
                logging.info(f"Received DONE signal from Crawler {source}")
                crawler_tasks_assigned[0] -= 1
                active_crawlers.append(source)

                completed_task_id = message_data.get("task_id")

                # del task_dict[completed_task_id]

                crawler_tasks[source] = "def"
                logging.info(f"Removed completed task ID: {completed_task_id} from remaining_tasks")
                logging.info(f"{crawler_tasks_assigned[0]} > 0 or {num_tasks[0]} > 0")

            elif tag == 2:
                logging.info(f"Received Start signal from Crawler {source}")
                crawler_tasks[source] = message_data


            elif tag == 11:
                logging.info(f"Received DONE signal from Indexer {source}")
                logging.info(f"Search results for '{remaining_keywords[0]}': {message_data}")
                logging.info(f"Removed completed keyword: {remaining_keywords[0]} from remaining keywords")
                # remaining_keywords = remaining_keywords[1:]
                # indexer_tasks_assigned += 1
                num_keywords -= 1
                # active_indexers.remove(indexer_rank)
                indexer_is_available = True

            elif tag == 99:
                logging.info(f"Received URLs from crawler {source}")
                discovered_urls.extend(message_data)
            elif tag == 999:
                logging.error(f"Crawler {source} reported error: {message_data}")
                crawler_tasks_assigned[0] -= 1

        time.sleep(0.1)

    logging.info(f"Master node finished dispatching all tasks. Discovered {len(discovered_urls)} URLs. Exiting.")
    stop_event.set()
    ping_thread.join()

# Run Flask server in a separate thread
flask_thread = threading.Thread(target=start_flask_server)
flask_thread.start()

if __name__ == '__main__':
    master_process()
