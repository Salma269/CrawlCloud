from mpi4py import MPI
import time
import logging
import boto3
import threading
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/696726802797/TaskQueueForCrawlers'  

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

def send_seed_tasks_to_queue(seed_tasks):
    message_ids = []
    for task in seed_tasks:
        message_id = send_to_queue(task)
        if message_id:
            message_ids.append(message_id)  
        else:
            logging.error("Failed to send task to SQS")
    return message_ids

ping_lock = threading.Lock()
stop_event = threading.Event()

def ping_thread_func(comm, ping_interval, status, crawler_tasks, active_crawlers, num_tasks, task_dict, c, crawler_tasks_assigned):
    while not stop_event.is_set():
        crawlers_that_responded = []

        ping_deadline = time.time() + 5
        while time.time() < ping_deadline:
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=101, status=status):
                source = status.Get_source()
                comm.recv(source=source, tag=101)
                c[0] += 1
                logging.info(f"{c[0]} Received pong from Crawler {source}")
                if source not in crawlers_that_responded:
                    crawlers_that_responded.append(source)
            else:
                time.sleep(0.05)

        if 1 in crawlers_that_responded:
            logging.info(f"Crawler responded")
            crawlers_that_responded.remove(1)
        else:
            if crawler_tasks[1] != "def":
                logging.error(f"Crawler did not respond to ping.")
                num_tasks[0] += 1
                send_seed_tasks_to_queue([task_dict[crawler_tasks[1]]])
                logging.info(f"Reuploaded task from failed crawler 1")
                crawler_tasks_assigned[0] -= 1
                del task_dict[crawler_tasks[1]]
                crawler_tasks[1] = "def"
                
        comm.send("ping", dest=1, tag=100)
        logging.info(f"Sent ping to Crawler 1")
        time.sleep(0.1)

def master_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()

    logging.info(f"Master node started with rank {rank} of {size}")

    client_rank = size - 1
    indexer_rank = size - 2
    crawler_nodes = size - 3

    if crawler_nodes <= 0:
        logging.error("Need at least 3 total nodes: master, 1+ crawler, 1 indexer, 1 client.")
        return

    active_crawlers = list(range(1, 1 + crawler_nodes))
    active_indexers = [indexer_rank]

    seed_tasks = [
        {"url": "http://quotes.toscrape.com/", "allowed_domains": ["toscrape.com"], "obey_robots": True, "delay": 1.0, "depth": 1},
        {"url": "http://books.toscrape.com/", "allowed_domains": ["toscrape.com"], "obey_robots": True, "delay": 1.0, "depth": 1}
    ]

    keywords = ["hello", "this", "python"]

    remaining_tasks = send_seed_tasks_to_queue(seed_tasks)
    num_tasks = [len(remaining_tasks)] 
    task_dict = dict(zip(remaining_tasks, seed_tasks))
    task_dict["def"] = "yarab"

    remaining_keywords = keywords
    num_keywords = len(remaining_keywords)

    crawler_tasks_assigned = [0]
    indexer_is_available = True
    discovered_urls = []

    crawler_tasks = ["def"] * (crawler_nodes + 1)

    c = [0]
    ping_thread = threading.Thread(target=ping_thread_func, args=(
        comm, 2, status, crawler_tasks, active_crawlers, num_tasks, task_dict, c, crawler_tasks_assigned))
    ping_thread.start()

    last_client_keyword = None
    last_client_query_pending = False

    while crawler_tasks_assigned[0] > 0 or num_tasks[0] > 0 or last_client_query_pending:
        for crawler_rank in active_crawlers:
            if num_tasks[0] > 0:
                comm.send("start", dest=crawler_rank, tag=42)
                crawler_tasks_assigned[0] += 1
                num_tasks[0] -= 1
                active_crawlers.remove(crawler_rank)
                logging.info(f"Sent start signal to Crawler {crawler_rank} ")

        if num_keywords > 0 and indexer_is_available:
            comm.send(remaining_keywords[0], dest=indexer_rank, tag=10)
            indexer_is_available = False
            logging.info(f"Sent start signal to Indexer {indexer_rank} ")

        while comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            source = status.Get_source()
            tag = status.Get_tag()
            message_data = comm.recv(source=source, tag=tag)

            if tag == 1:
                logging.info(f"Received DONE signal from Crawler {source}")
                crawler_tasks_assigned[0] -= 1
                active_crawlers.append(source)
                completed_task_id = message_data.get("task_id")
                crawler_tasks[source] = "def"
                logging.info(f"Removed completed task ID: {completed_task_id}")

            elif tag == 2:
                logging.info(f"Received Start signal from Crawler {source}")
                crawler_tasks[source] = message_data

            elif tag == 11:
                logging.info(f"Received DONE signal from Indexer {source}")
                logging.info(f"Search results: {message_data}")
                indexer_is_available = True
                if last_client_query_pending:
                    comm.send(message_data, dest=client_rank, tag=92)
                    logging.info(f"Sent search result to client for keyword: {last_client_keyword}")
                    last_client_query_pending = False
                else:
                    num_keywords -= 1
                    remaining_keywords = remaining_keywords[1:]
          elif tag == 90:  # URL submission from client
                url = message_data["data"]
                task = {
                    "url": url,
                    "allowed_domains": ["custom-domain.com"],
                    "obey_robots": True,
                    "delay": 1.0,
                    "depth": 1
                }
                message_id = send_to_queue(task)
                if message_id:
                    task_dict[message_id] = task
                    num_tasks[0] += 1
                    logging.info(f"Received new URL from client: {url}")

          elif tag == 91:  # Search query from client
                keyword = message_data["data"]
                if indexer_is_available:
                    comm.send(keyword, dest=indexer_rank, tag=10)
                    indexer_is_available = False
                    last_client_keyword = keyword
                    last_client_query_pending = True
                    logging.info(f"Received search query from client: {keyword}")
                else:
                    logging.warning("Indexer busy. Ignoring client search request for now.")

          elif tag == 99:
                logging.info(f"Received URLs from crawler {source}")
                discovered_urls.extend(message_data)

          elif tag == 999:
                logging.error(f"Crawler {source} reported error: {message_data}")
                crawler_tasks_assigned[0] -= 1

        time.sleep(0.1)

    logging.info(f"Master node finished. Discovered {len(discovered_urls)} URLs. Exiting.")
    stop_event.set()
    ping_thread.join()

if __name__ == '__main__':
    master_process()
