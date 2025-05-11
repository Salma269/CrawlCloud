import subprocess

INPUT_HOSTFILE = "nodes.txt"
OUTPUT_HOSTFILE = "reachable_hosts.txt"
SSH_USER = "ubuntu"  

def is_ssh_reachable(ip):
    try:
        result = subprocess.run(
            ["ssh", f"{SSH_USER}@{ip}", "echo ok"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=5
        )
        return result.stdout.strip() == b"ok"
    except Exception:
        return False

def read_hosts(filename):
    with open(filename, 'r') as f:
        return [line.strip().split()[0] for line in f if line.strip()]

def write_hosts(hosts, filename):
    with open(filename, 'w') as f:
        f.write("172.31.80.40 slots=1\n")
        for ip in hosts:
            f.write(f"{ip} slots=1\n")

def main():
    print("[Launcher] Reading nodes.txt...")
    hosts = read_hosts(INPUT_HOSTFILE)

    print("[Launcher] Checking SSH access...")
    reachable_hosts = [ip for ip in hosts if is_ssh_reachable(ip)]

    if not reachable_hosts:
        print("[Launcher] ERROR: No SSH-accessible hosts found.")
        return

    print(f"[Launcher] Reachable nodes: {reachable_hosts}")

    write_hosts(reachable_hosts, OUTPUT_HOSTFILE)

    np = len(reachable_hosts) + 1
    print(f"[Launcher] Launching mpirun with {np} processes...")

    try:
        subprocess.run([
            "mpirun",
            "--hostfile", OUTPUT_HOSTFILE,
            "-np", str(np),
            "python3", "main.py"
        ], check=True)
    except subprocess.CalledProcessError as e:
        print(f"[Launcher] mpirun failed: {e}")

if __name__ == "__main__":
    main()
