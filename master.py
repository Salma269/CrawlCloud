from mpi4py import MPI
import time
import logging
import boto3  # For AWS SQS
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

# Initialize SQS client
sqs = boto3.client('sqs', region_name='us-west-2')  # Adjust region as needed
queue_url = 'https://sqs.us-west-2.amazonaws.com/your-account-id/your-queue-name'  # Replace with your SQS queue URL

def send_to_queue(message):
    """
    Send a message to the task queue (SQS).
    """
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )
        logging.info(f"Message sent to SQS, Message ID: {response['MessageId']}")
    except Exception as e:
        logging.error(f"Failed to send message to SQS: {e}")

def master_process():
    """
    Main process for the master node.
    Handles task distribution, worker management, and coordination.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()
    logging.info(f"Master node started with rank {rank} of {size}")
    
    # Initialize task queue, database connections, etc.
    crawler_nodes = size - 2  # Assuming master and at least one indexer node
    indexer_nodes = 1  # At least one indexer node
    if crawler_nodes <= 0 or indexer_nodes <= 0:
        logging.error("Not enough nodes to run crawler and indexer. Need at least 3 nodes (1 master, 1 crawler, 1 indexer)")
        return

    active_crawler_nodes = list(range(1, 1 + crawler_nodes))  # Ranks for crawler nodes (assuming rank 0 is master)
    active_indexer_nodes = list(range(1 + crawler_nodes, size))  # Ranks for indexer nodes
    logging.info(f"Active Crawler Nodes: {active_crawler_nodes}")
    logging.info(f"Active Indexer Nodes: {active_indexer_nodes}")

    seed_urls = ["http://example.com", "http://example.org"]  # Example seed URLs - replace with actual seed URLs
    # Each URL becomes a tuple (url, depth)
    urls_to_crawl_queue = [(url, 0) for url in seed_urls]  # depth=0 for seeds
    task_count = 0
    crawler_tasks_assigned = 0
    
    while urls_to_crawl_queue or crawler_tasks_assigned > 0:  # Continue as long as there are URLs to crawl or tasks in progress
        # Check for completed crawler tasks and results from crawler nodes
        if crawler_tasks_assigned > 0:
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):  # Non-blocking check for incoming messages
                message_source = status.Get_source()
                message_tag = status.Get_tag()
                message_data = comm.recv(source=message_source, tag=message_tag)
                
                if message_tag == 1:  # Crawler completed task and sent back extracted URLs
                    crawler_tasks_assigned -= 1
                    new_urls = message_data  # Assuming message_data is a list of (url, depth)
                    if new_urls:
                        urls_to_crawl_queue.extend(new_urls)  # Add newly discovered URLs to the queue
                    logging.info(f"Master received URLs from Crawler {message_source}, URLs in queue: {len(urls_to_crawl_queue)}, Tasks assigned: {crawler_tasks_assigned}")
                
                elif message_tag == 99:  # Crawler node reports status/heartbeat
                    logging.info(f"Crawler {message_source} status: {message_data}")  # Example status message
                
                elif message_tag == 999:  # Crawler node reports error
                    logging.error(f"Crawler {message_source} reported error: {message_data}")
                    crawler_tasks_assigned -= 1  # Decrement task count even on error, consider re-assigning task in real implementation
        
        # Assign new crawling tasks if there are URLs in the queue and available crawler nodes
        while urls_to_crawl_queue and crawler_tasks_assigned < crawler_nodes:  # Limit tasks to available crawler nodes for simplicity in this skeleton
            url_to_crawl, depth = urls_to_crawl_queue.pop(0)  # Get URL and depth from queue (FIFO for simplicity)
            
            # Create message with depth and timestamp
            message = {
                'url': url_to_crawl,
                'task_id': task_count,
                'depth': depth,
                'timestamp': time.time()  # Current UNIX timestamp
            }
            send_to_queue(message)
            task_count += 1
            
            # Round-robin assignment to crawler nodes
            available_crawler_rank = active_crawler_nodes[crawler_tasks_assigned % len(active_crawler_nodes)]
            comm.send(message, dest=available_crawler_rank, tag=0)  # Send full message (not just URL) now
            crawler_tasks_assigned += 1
            logging.info(f"Master assigned task {task_count} (crawl {url_to_crawl}) to Crawler {available_crawler_rank}, Tasks assigned: {crawler_tasks_assigned}")
        
        time.sleep(0.1)  # Small delay to prevent overwhelming master
        time.sleep(1)  # Master node's main loop sleep
        logging.info("Master node finished URL distribution. Waiting for crawlers to complete...")

    logging.info("Master Node Finished.")

if _name_ == '_main_':
    master_process()
