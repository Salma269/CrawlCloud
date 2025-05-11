from mpi4py import MPI
import subprocess
import socket
import traceback
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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

# IP-to-role mapping
ROLE_MAP = {
    '172.31.80.40': 'master',
    '172.31.80.247': 'crawler',
    '172.31.86.107': 'crawler',
    '172.31.21.53': 'indexer'
}

# Initialize MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

# Identify local IP and role
local_ip = get_local_ip()
role = ROLE_MAP.get(local_ip, 'unknown')

print(f"[Rank {rank}] Local IP: {local_ip}, Role: {role}")
logging.info(f"[Rank {rank}] Local IP: {local_ip}, Role: {role}")
print(f"[Rank {rank}] Detected IP: {local_ip} | Role from map: {ROLE_MAP.get(local_ip, 'unknown')}")

# Master process
if role == 'master':
    print(f"[Rank {rank}] Running master process...")
    try:
        from masterscript import master_process
        master_process()
        print("[Master] Finished master_process")
    except Exception as e:
        error_msg = f"[{local_ip}] Master failed:\n{traceback.format_exc()}"
        logging.error(error_msg)
        comm.send(error_msg, dest=0, tag=97)

# Indexer node
elif role == 'indexer':
    print(f"[Rank {rank}] Indexer node starting...")
    try:
        from indexer import indexer_process
        logging.info("Starting indexer_process...")
        indexer_process()
        logging.info("Indexer process completed.")
        comm.send(f"[{local_ip}] Indexer finished successfully.", dest=0, tag=12)
    except Exception as e:
        error_msg = f"[{local_ip}] Indexer failed:\n{traceback.format_exc()}"
        logging.error(error_msg)
        comm.send(error_msg, dest=0, tag=98)

# Unknown role
else:
    print(f"[Rank {rank}] Unknown role for IP {local_ip}")
    error_msg = f"[Rank {rank}] Unknown role for IP: {local_ip}"
    logging.error(error_msg)
    comm.send(error_msg, dest=0, tag=97)
