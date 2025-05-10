from mpi4py import MPI
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - Client - %(levelname)s - %(message)s')

def client_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    if rank != comm.Get_size() - 1:
        logging.error("This process is not the client. Exiting.")
        return

    logging.info(f"Client started on rank {rank}")

    # Send a URL to the master
    url = "http://example.com"
    comm.send({"type": "url", "data": url}, dest=0, tag=90)
    logging.info(f"Sent URL to master: {url}")

    time.sleep(2)

    # Send a keyword for search
    keyword = "python"
    comm.send({"type": "keyword", "data": keyword}, dest=0, tag=91)
    logging.info(f"Sent keyword to master: {keyword}")

    # Wait for result from master
    status = MPI.Status()
    while True:
        if comm.iprobe(source=0, tag=92, status=status):
            result = comm.recv(source=0, tag=92)
            logging.info(f"Received search result: {result}")
            break
        time.sleep(0.5)

if __name__ == '__main__':
    client_process()
