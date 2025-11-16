import os
import random
import socket
import time
from datetime import datetime
import shutil

NUM_LINES = 100                     
SPOOL_DIR = "/spooldir"             
TMP_DIR = "/tmp"         

os.makedirs(SPOOL_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

def generate_log_line():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    invoice_no = random.randint(100000, 999999)
    server_ip = socket.gethostbyname(socket.gethostname())

    status_code = random.choice([200, 201, 400, 404, 500])

    return f"{timestamp},{invoice_no},{server_ip},{status_code}\n"


def main():
    date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    tmp_file = f"{TMP_DIR}/log_{date_str}.log"
    final_file = f"{SPOOL_DIR}/log_{date_str}.log"

    print(f"Generating log file: {tmp_file}")

    # Write fully inside /tmp (Flume requirement)
    with open(tmp_file, "w") as f:
        f.write("timestamp,InvoiceNo,server_ip,status_code\n")
        for _ in range(NUM_LINES):
            f.write(generate_log_line())

    shutil.move(tmp_file, final_file)

    print(f"Log file successfully moved to: {final_file}")
    print("Flume will process it automatically.")

if __name__ == "__main__":
    main()