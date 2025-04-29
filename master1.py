from mpi4py import MPI
import time
import logging
import boto3
import json
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

# AWS SQS setup
sqs = boto3.client('sqs', region_name='us-west-2')
queue_url = 'https://sqs.us-west-2.amazonaws.com/your-account-id/your-queue-name'  # Replace with actual

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

    seed_urls = ["http://example.com", "http://example.org"]
    urls_to_crawl = [(url, 0) for url in seed_urls]
    task_status = {}  # task_id -> {crawler, timestamp, message}
    task_id = 0
    timeout_seconds = 15

    while True:
        # --- CLIENT INPUT ---
        try:
            user_input = input("Enter command (url <url> / search <keyword>): ")
            if user_input.startswith("url "):
                url = user_input[4:].strip()
                urls_to_crawl.append((url, 0))
            elif user_input.startswith("search "):
                keyword = user_input[7:].strip()
                comm.send(keyword, dest=indexer_rank, tag=10)
                result = comm.recv(source=indexer_rank, tag=11)
                logging.info(f"Search results for '{keyword}': {result}")
        except EOFError:
            break  # Allow clean exit

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
    master_process()
