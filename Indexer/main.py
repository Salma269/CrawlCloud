import mpi4py
mpi4py.rc.initialize = False  # Disable auto-initialization
from mpi4py import MPI

# Initialize MPI once for all roles
required_thread_level = MPI.THREAD_MULTIPLE
current_level = MPI.Init_thread(required_thread_level)
if current_level < required_thread_level:
    raise RuntimeError(f"MPI_THREAD_MULTIPLE not supported (level {current_level})")

comm = MPI.COMM_WORLD
status = MPI.Status()
rank = comm.Get_rank()

# Rest of the imports
import socket
import traceback
import logging
import threading
import time
from indexer import BasicIndexerNode, fetch_from_sqs  # Import necessary components from indexer.py

logging.basicConfig(level=logging.INFO, format="%(asctime)s - Indexer - %(levelname)s - %(message)s")

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

ROLE_MAP = {
    '172.31.80.40': 'master',
    '172.31.80.247': 'crawler',
    '172.31.21.53': 'indexer',
    '172.31.30.16': 'client'
}

local_ip = get_local_ip()
role = ROLE_MAP.get(local_ip, 'unknown')

if role == 'master' and rank == 0:
    from masterscript import master_process
    master_process()

elif role == 'crawler':
    from twisted.internet import reactor
    from scrapy.crawler import CrawlerRunner
    from scrapy.utils.project import get_project_settings
    from scrapy.utils.log import configure_logging
    from webcrawler.webcrawler.spiders.mycrawler import CrawlingSpider

    # Configure Scrapy logging
    configure_logging()
    
    # Initialize crawler system
    settings = get_project_settings()
    runner = CrawlerRunner(settings)
    crawling_in_progress = False
    crawl_lock = threading.Lock()

    def trigger_listener():
        """Dedicated thread for handling crawl triggers"""
        while True:
            try:
                # Blocking receive for crawl triggers (tag 42)
                if comm.Iprobe(source=0, tag=42, status=status):
                    trigger = comm.recv(source=0, tag=42)
                    logging.info(f"Received crawl trigger: {trigger}")

                    with crawl_lock:
                        if not crawling_in_progress:
                            # Schedule in reactor thread
                            reactor.callFromThread(start_crawl, trigger)
                        else:
                            comm.send(f"[{local_ip}] Crawler busy, skipping trigger", 
                                      dest=0, tag=96)
            except Exception as e:
                error_msg = f"[{local_ip}] Trigger Error:\n{traceback.format_exc()}"
                comm.send(error_msg, dest=0, tag=99)

    def ping_listener():
        """Dedicated thread for handling pings"""
        c = 0
        while True:
            try:
                if comm.Iprobe(source=0, tag=100, status=status):
                    c += 1
                    # Blocking receive for pings (tag 100)
                    message = comm.recv(source=0, tag=100)
                    if message == "ping":
                        comm.send("pong", dest=0, tag=101)
                        logging.info(f"Responded to ping {c} from master")
            except Exception as e:
                error_msg = f"[{local_ip}] Ping Error:\n{traceback.format_exc()}"
                comm.send(error_msg, dest=0, tag=99)

    def start_crawl(trigger):
        """Start the spider run (called in reactor thread)"""
        global crawling_in_progress
        with crawl_lock:
            crawling_in_progress = True
        logging.info("Starting spider run")
        deferred = runner.crawl(CrawlingSpider, trigger_data=trigger)
        deferred.addCallbacks(crawl_success, crawl_error)

    def crawl_success(result):
        """Handle successful crawl completion"""
        global crawling_in_progress
        with crawl_lock:
            crawling_in_progress = False
        comm.send(f"[{local_ip}] Crawler finished successfully", dest=0, tag=11)
        logging.info("Spider finished successfully")
        return result

    def crawl_error(failure):
        """Handle crawl failures"""
        global crawling_in_progress
        with crawl_lock:
            crawling_in_progress = False
        error_msg = f"[{local_ip}] Crawler failed:\n{failure.getTraceback()}"
        comm.send(error_msg, dest=0, tag=99)
        logging.error("Spider failed: %s", failure.getErrorMessage())
        return failure

    # Start dedicated listener threads
    threading.Thread(target=ping_listener, daemon=True).start()
    threading.Thread(target=trigger_listener, daemon=True).start()
    
    # Start reactor (main thread)
    reactor.run()

elif role == 'indexer':
    # Initialize indexer
    indexer = BasicIndexerNode(use_sql=True)
    indexing_in_progress = False
    index_lock = threading.Lock()
    SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/696726802797/ScrapeQueue"

    def ping_listener():
        """Dedicated thread for handling pings"""
        c = 0
        while True:
            try:
                if comm.Iprobe(source=0, tag=100, status=status):
                    c += 1
                    # Blocking receive for pings (tag 100)
                    message = comm.recv(source=0, tag=100)
                    if message == "ping":
                        comm.send("pong", dest=0, tag=101)
                        logging.info(f"Responded to ping {c} from master")
            except Exception as e:
                error_msg = f"[{local_ip}] Ping Error:\n{traceback.format_exc()}"
                comm.send(error_msg, dest=0, tag=98)

    def sqs_listener():
        """Dedicated thread for handling SQS tasks"""
        global indexing_in_progress  # Declare global at the start
        while True:
            try:
                # Poll SQS for messages
                items = fetch_from_sqs(SQS_QUEUE_URL)
                if items:
                    with index_lock:
                        if not indexing_in_progress:
                            indexing_in_progress = True
                            for url, text in items:
                                logging.info(f"Ingesting content from {url}")
                                indexer.ingest_from_crawler(url, text)
                            indexing_in_progress = False
                        else:
                            comm.send(f"[{local_ip}] Indexer busy, queuing SQS task", dest=0, tag=96)
                else:
                    time.sleep(0.1)  # Avoid busy looping when no messages
            except Exception as e:
                error_msg = f"[{local_ip}] SQS Processing Error:\n{traceback.format_exc()}"
                comm.send(error_msg, dest=0, tag=98)
                indexing_in_progress = False

    def search_listener():
        """Dedicated thread for handling search queries"""
        while True:
            try:
                if comm.Iprobe(source=0, tag=10, status=status):
                    keyword = comm.recv(source=0, tag=10)
                    logging.info(f"Received search query: {keyword}")
                    results = indexer.search(keyword)
                    comm.send(results, dest=0, tag=11)
                    logging.info(f"Sent search results to master: {results}")
            except Exception as e:
                error_msg = f"[{local_ip}] Search Error:\n{traceback.format_exc()}"
                comm.send(error_msg, dest=0, tag=98)

    try:
        # Start dedicated listener threads
        threading.Thread(target=ping_listener, daemon=True).start()
        threading.Thread(target=sqs_listener, daemon=True).start()
        threading.Thread(target=search_listener, daemon=True).start()

        # Keep main thread alive
        while True:
            time.sleep(1)
            # Check if all threads are still alive (optional)
            if not any(t.is_alive() for t in threading.enumerate() if t is not threading.current_thread()):
                raise Exception("All listener threads have terminated")

        comm.send(f"[{local_ip}] Indexer finished successfully.", dest=0, tag=12)
    except Exception as e:
        error_msg = f"[{local_ip}] Indexer failed:\n{traceback.format_exc()}"
        comm.send(error_msg, dest=0, tag=98)

elif role == 'client':
    try:
        from client import client_process
        client_process()
    except Exception as e:
        error_msg = f"[{local_ip}] Client failed:\n{traceback.format_exc()}"
        comm.send(error_msg, dest=0, tag=98)

else:
    error_msg = f"[Rank {rank}] Unknown role for IP: {local_ip}"
    comm.send(error_msg, dest=0, tag=97)
