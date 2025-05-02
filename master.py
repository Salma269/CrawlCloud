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


def foo():
    while True:
        pass

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

def ping_thread_func(comm, ping_interval, status, crawler_tasks, active_crawlers, num_tasks, task_dict):
    #global inactive_crawlers
    while not stop_event.is_set():
        #with ping_lock:
        #    inactive_crawlers

        crawlers_that_responded = []

        # Wait and listen for pong replies for `ping_interval` seconds
        ping_deadline = time.time() + 5
        while time.time() < ping_deadline:
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=101, status=status):
                source = status.Get_source()
                comm.recv(source=source, tag=101)
                logging.info(f"Received pong from Crawler {source}")
    
                if source not in crawlers_that_responded:
                    crawlers_that_responded.append(source)
            else:
                time.sleep(0.05)

    
        #unresponsive = [c for c in active_crawlers if c not in crawlers_that_responded]
        #for crawler in unresponsive:
        if 1 in crawlers_that_responded:
            logging.info(f"Crawler responded")
            crawlers_that_responded.remove(1)
        else:
            if crawler_tasks[1] != "def":
                logging.error(f"Crawler did not respond to ping.")
                foo()
                num_tasks[0] += 1
                send_seed_tasks_to_queue([task_dict[crawler_tasks[1]]])
                logging.info(f"Reuploaded task from failed crawler 1")
                crawler_tasks_assigned -= 1
                active_crawlers.append(source)
                crawler_tasks[1] = "def"
                del task_dict[completed_task_id]
                
                
            
        comm.send("ping", dest=1, tag=100)
        logging.info(f"Sent piiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiing to Crawler 1")

        time.sleep(0.1)  # Slight delay before next ping loop

def master_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()

    logging.info(f"Master node started with rank {rank} of {size}")

    crawler_nodes = size - 2
    indexer_nodes = 1
    if crawler_nodes <= 0 or indexer_nodes <= 0:
        logging.error("Need at least 3 nodes: 1 master, 1 crawler, 1 indexer.")
        return

    active_crawlers = list(range(1, 1 + crawler_nodes))
    active_indexers = list(range(1 + crawler_nodes, size))   

    seed_tasks = [
        {"url": "http://quotes.toscrape.com/", "allowed_domains": ["toscrape.com"], "obey_robots": True, "delay": 1.0, "depth": 1},
        {"url": "http://quotes.toscrape.com/", "allowed_domains": ["toscrape.com"], "obey_robots": True, "delay": 1.0, "depth": 1}
        #{"url": "http://books.toscrape.com/", "allowed_domains": ["toscrape.com"], "obey_robots": True, "delay": 1.0, "depth": 1},

    ]

    keywords = ["hello", "this", "python"]

    remaining_tasks = send_seed_tasks_to_queue(seed_tasks)
    num_tasks = [len(remaining_tasks)] 
    task_dict = dict(zip(remaining_tasks, seed_tasks))

    task_dict["def"] = "yarab"

    remaining_keywords = keywords
    num_keywords = len(remaining_keywords)

    crawler_tasks_assigned = 0
    #indexer_tasks_assigned = 0
    indexer_is_available = True
    discovered_urls = []

    crawler_tasks = ["def"] * (crawler_nodes+1)

    ping_thread = threading.Thread(target=ping_thread_func, args=(comm, 2, status, crawler_tasks, active_crawlers, num_tasks, task_dict))
    ping_thread.start()

    while crawler_tasks_assigned > 0 or num_tasks[0] > 0:

        #logging.info(f"{task_dict[crawler_tasks[1]]} -> {crawler_tasks[1]} -> {crawler_tasks}")
        for crawler_rank in active_crawlers:
            if num_tasks[0] > 0:
                comm.send("start", dest=crawler_rank, tag=42)
                crawler_tasks_assigned += 1
                num_tasks[0] -= 1
                active_crawlers.remove(crawler_rank)
                logging.info(f"Sent start signal to Crawler {crawler_rank} ")

        #for indexer_rank in active_indexers:
        if num_keywords > 0:
            if indexer_is_available:
                comm.send(remaining_keywords[0], dest=2, tag=10)
                #remaining_keywords = remaining_keywords[1:]
                #indexer_tasks_assigned += 1
                #num_keywords -= 1
                #active_indexers.remove(indexer_rank)
                logging.info(f"Sent start signal to Indexer {2} ")
                indexer_is_available = False

        while comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            source = status.Get_source()
            tag = status.Get_tag()
            message_data = comm.recv(source=source, tag=tag)

            #if ping_sent_time is None or time.time() - ping_sent_time >= 2:
            #    logging.info("WWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW")
            #    crawlers_that_responded = []
            #    if ping_sent_time is not None:
            #        while comm.iprobe(source=MPI.ANY_SOURCE, tag=101, status=status):
            #            source = status.Get_source()
            #            comm.recv(source=source, tag=101)
            #            logging.info(f"Crawler SENT POOOOOOOOOOOOOOOOOOOONG.")
            #            if source not in crawlers_that_responded:
            #                crawlers_that_responded.append(source)

            #        #for crawler in active_crawlers:
            #        if 1 not in crawlers_that_responded:
            #            logging.error(f"Crawler did not respond with pong within 10 seconds.")
            #            active_crawlers.remove(crawler)

                #change when you add crawlers
            #   logging.info("11111111111111111111111111111111111111111111")
            #    comm.send("ping", dest=1, tag=100)
            #    logging.info(f"Sent ping to Crawler 1")
            #    ping_sent_time = time.time()

            if tag == 1:
                logging.info(f"Received DONE signal from Crawler {source}")
                crawler_tasks_assigned -= 1
                active_crawlers.append(source)

                completed_task_id = message_data.get("task_id")

                del task_dict[completed_task_id]

                crawler_tasks[source] = "def"

                logging.info(f"Removed completed task ID: {completed_task_id} from remaining_tasks")

            elif tag == 2:
                logging.info(f"Received Start signal from Crawler {source}")
                crawler_tasks[source] = message_data

            elif tag == 11:
                logging.info(f"Received DONE signal from Indexer {source}")
                logging.info(f"Search results for '{remaining_keywords[0]}': {message_data}")
                logging.info(f"Removed completed keyword: {remaining_keywords[0]} from remaining keywords")
                remaining_keywords = remaining_keywords[1:]
                #indexer_tasks_assigned += 1
                num_keywords -= 1
                #active_indexers.remove(indexer_rank)
                indexer_is_available = True

            elif tag == 99:
                logging.info(f"Received URLs from crawler {source}")
                discovered_urls.extend(message_data)

            elif tag == 999:
                logging.error(f"Crawler {source} reported error: {message_data}")
                crawler_tasks_assigned -= 1

        

        time.sleep(0.1)

    logging.info(f"Master node finished dispatching all tasks. Discovered {len(discovered_urls)} URLs. Exiting.")
    stop_event.set()
    ping_thread.join()
    #comm.send("start", dest=crawler_rank, tag=42)



if __name__ == '__main__':
    master_process()

