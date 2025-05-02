# Add these at the VERY TOP of the file
import mpi4py
mpi4py.rc.initialize = False  # Disable auto-initialization
from mpi4py import MPI

# Initialize MPI once for all roles
required_thread_level = MPI.THREAD_MULTIPLE
current_level = MPI.Init_thread(required_thread_level)
if current_level < required_thread_level:
    raise RuntimeError(f"MPI_THREAD_MULTIPLE not supported (level {current_level})")

comm = MPI.COMM_WORLD
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

# Add your spider import (modify to match your actual spider location)
from webcrawler.webcrawler.spiders.mycrawler import CrawlingSpider

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
    '172.31.21.53': 'indexer'
}

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
local_ip = get_local_ip()
role = ROLE_MAP.get(local_ip, 'unknown')

if role == 'master' and rank == 0:
    from masterscript import master_process
    master_process()

elif role == 'crawler':
    # Configure Scrapy logging
    configure_logging()
    
    # Initialize crawler system
    settings = get_project_settings()
    runner = CrawlerRunner(settings)
    crawling_in_progress = False

    # Add these helper functions
    def check_mpi_messages():
        """Check for both triggers and pings from the reactor loop"""
        status = MPI.Status()
        
        # Check for crawl triggers (tag 42)
        if comm.Iprobe(source=0, tag=42, status=status):
            handle_crawl_trigger(status)
        
        # Check for pings (tag 100)
        if comm.Iprobe(source=0, tag=100, status=status):
            handle_ping(status)
        
        # Schedule next check
        reactor.callLater(0.1, check_mpi_messages)

    def handle_crawl_trigger(status):
        """Handle incoming crawl requests"""
        global crawling_in_progress
        try:
            trigger = comm.recv(source=0, tag=42)
            logging.info(f"Received crawl trigger: {trigger}")
            
            if not crawling_in_progress:
                start_crawl(trigger)
            else:
                comm.send(f"[{local_ip}] Crawler busy, skipping trigger", dest=0, tag=96)
                
        except Exception as e:
            error_msg = f"[{local_ip}] Trigger Error:\n{traceback.format_exc()}"
            comm.send(error_msg, dest=0, tag=99)

    def handle_ping(status):
        """Handle ping requests"""
        try:
            message = comm.recv(source=0, tag=100)
            if message == "ping":
                comm.send("pong", dest=0, tag=101)
                logging.info("Responded to ping from master")
        except Exception as e:
            error_msg = f"[{local_ip}] Ping Error:\n{traceback.format_exc()}"
            comm.send(error_msg, dest=0, tag=99)

    def start_crawl(trigger):
        global crawling_in_progress
        crawling_in_progress = True
        logging.info("Starting spider run")
        
        deferred = runner.crawl(CrawlingSpider, trigger_data=trigger)
        deferred.addCallbacks(crawl_success, crawl_error)

    def crawl_success(result):
        global crawling_in_progress
        crawling_in_progress = False
        comm.send(f"[{local_ip}] Crawler finished successfully", dest=0, tag=11)
        logging.info("Spider finished successfully")
        return result

    def crawl_error(failure):
        global crawling_in_progress
        crawling_in_progress = False
        error_msg = f"[{local_ip}] Crawler failed:\n{failure.getTraceback()}"
        comm.send(error_msg, dest=0, tag=99)
        logging.error("Spider failed: %s", failure.getErrorMessage())
        return failure

    # Start periodic MPI checks in reactor context
    check_mpi_messages()
    
    # Start reactor
    reactor.run()

elif role == 'indexer':
    try:
        from indexer import indexer_process
        indexer_process()
        comm.send(f"[{local_ip}] Indexer finished successfully.", dest=0, tag=12)
    except Exception as e:
        error_msg = f"[{local_ip}] Indexer failed:\n{traceback.format_exc()}"
        comm.send(error_msg, dest=0, tag=98)

else:
    error_msg = f"[Rank {rank}] Unknown role for IP: {local_ip}"
    comm.send(error_msg, dest=0, tag=97)