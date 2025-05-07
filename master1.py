from mpi4py import MPI
import time
import logging
import boto3
import json
from flask import Flask, request, render_template_string
from collections import defaultdict
from threading import Thread

# AWS SQS setup
sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/696726802797/TaskQueueForCrawlers'  

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

app = Flask(__name__)

# Web server setup
app = Flask(__name__)
latest_search_result = {}

@app.route("/search", methods=["POST"])
def search():
    keyword = request.json.get("keyword")
    if not keyword:
        return jsonify({"error": "Missing keyword"}), 400

    indexer_rank = size - 1  # Last rank assumed to be indexer
    comm.send(keyword, dest=indexer_rank, tag=10)
    result = comm.recv(source=indexer_rank, tag=11)
    logging.info(f"Search results for '{keyword}': {result}")
    return jsonify({"results": result})
def run_web_server():
    app.run(host="0.0.0.0", port=5000)

def send_to_queue(message):
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )
        logging.info(f"Sent message to SQS. ID: {response['MessageId']}")
    except Exception as e:
        logging.error(f"SQS Error: {e}")

def master_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()

    logging.info(f"Master node started on rank {rank} with {size} total processes")

    crawler_nodes = size - 2
    indexer_nodes = 1

    if crawler_nodes <= 0 or indexer_nodes <= 0:
        logging.error("Need at least 3 nodes: master, crawler, and indexer")
        return

    active_crawlers = list(range(1, 1 + crawler_nodes))
    indexer_rank = 1 + crawler_nodes

    logging.info(f"Crawlers: {active_crawlers}, Indexer: {indexer_rank}")
# Launch web server in background
    Thread(target=run_web_server, daemon=True).start()
    logging.info("Web client started at http://<this-ec2-ip>:5000")

    seed_urls = ["http://example.com", "http://example.org"]
    urls_to_crawl = [(url, 0) for url in seed_urls]
    task_status = {}  # task_id -> {crawler, timestamp, message}
    task_id = 0
    timeout_seconds = 15

    while True:
        # --- Handle Web Client Input (Search or URL) ---
        @app.route("/", methods=["GET", "POST"])
        def index():
            if request.method == "POST":
                user_input = request.form["query"]
                if user_input.startswith("url "):  # Handle URL addition
                    url = user_input[4:].strip()
                    urls_to_crawl.append((url, 0))
                    return render_template_string(open("templates/index.html").read(), results="URL added successfully.")
                elif user_input.startswith("search "):  # Handle Search query
                    keyword = user_input[7:].strip()
                    comm.send(keyword, dest=indexer_rank, tag=10)
                    result = comm.recv(source=indexer_rank, tag=11)
                    return render_template_string(open("templates/index.html").read(), results=f"Search results for '{keyword}': {result}")
            return render_template_string(open("templates/index.html").read(), results=None)

        # --- HANDLE COMPLETED TASKS --- 
        while comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            source = status.Get_source()
            tag = status.Get_tag()
            data = comm.recv(source=source, tag=tag)

            if tag == 1:  # Crawling result
                completed_task_ids = [tid for tid, v in task_status.items() if v['crawler'] == source]
                for tid in completed_task_ids:
                    task_status.pop(tid)
                new_urls = data  # list of (url, depth)
                urls_to_crawl.extend(new_urls)
                logging.info(f"Received {len(new_urls)} URLs from crawler {source}")

            elif tag == 99:
                logging.info(f"Heartbeat from crawler {source}: {data}")

            elif tag == 999:
                logging.error(f"Crawler error from {source}: {data}")

        # --- ASSIGN TASKS --- 
        while urls_to_crawl and len(task_status) < len(active_crawlers):
            url, depth = urls_to_crawl.pop(0)
            crawler = active_crawlers[len(task_status) % len(active_crawlers)]
            message = {"url": url, "depth": depth, "task_id": task_id, "timestamp": time.time()}

            # Send to SQS for redundancy / logging
            send_to_queue(message)

            # Send to crawler
            comm.send(message, dest=crawler, tag=0)
            task_status[task_id] = {"crawler": crawler, "timestamp": time.time(), "message": message}
            logging.info(f"Assigned task {task_id} to crawler {crawler}: {url}")
            task_id += 1

        # --- TIMEOUT & REQUEUE --- 
        now = time.time()
        timed_out = [tid for tid, v in task_status.items() if now - v['timestamp'] > timeout_seconds]
        for tid in timed_out:
            message = task_status[tid]['message']
            urls_to_crawl.append((message['url'], message['depth']))
            logging.warning(f"Requeued timed-out task {tid} from crawler {task_status[tid]['crawler']}")
            del task_status[tid]

        time.sleep(0.5)

if __name__ == '__main__':
    from threading import Thread
    # Start Flask app in a separate thread
    thread = Thread(target=app.run, kwargs={'host': '0.0.0.0', 'port': 5000})
    thread.start()

    # Start the master process
    master_process()
