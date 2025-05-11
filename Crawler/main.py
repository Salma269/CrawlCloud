import mpi4py
mpi4py.rc.initialize = False
from mpi4py import MPI

required_thread_level = MPI.THREAD_MULTIPLE
current_level = MPI.Init_thread(required_thread_level)
if current_level < required_thread_level:
    raise RuntimeError(f"MPI_THREAD_MULTIPLE not supported (level {current_level})")

comm = MPI.COMM_WORLD
status = MPI.Status()
rank = comm.Get_rank()

import subprocess
import socket
import traceback
import logging
from twisted.internet import reactor, defer, task
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
import threading
import time

from webcrawler.webcrawler.spiders.mycrawler import CrawlingSpider

logging.basicConfig(level=logging.INFO, format="%(asctime)s - Crawler - %(levelname)s - %(message)s")

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
    
    configure_logging()
    
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
                            pass
                            reactor.callFromThread(start_crawl, trigger)
                        else:
                            comm.send(f"[{local_ip}] Crawler busy, skipping trigger", 
                                    dest=0, tag=96)
                        
            except Exception as e:
                error_msg = f"[{local_ip}] Trigger Error:\n{traceback.format_exc()}"
                comm.send(error_msg, dest=0, tag=99)

    def ping_listener():
        """Dedicated thread for handling pings"""
        while True:
            try:
                if comm.Iprobe(source=0, tag=100, status=status):
                    #time.sleep(5)
                    # Blocking receive for pings (tag 100)
                    message = comm.recv(source=0, tag=100)
                    if message == "ping":
                        comm.send("pong", dest=0, tag=101)
                        logging.info("Responded to ping from master")
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
    try:
        from indexer import indexer_process
        indexer_process()
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
