#!/usr/bin/env python3
import random
import subprocess
from datetime import datetime, timezone
from pathlib import Path
import os

NUMBER_OF_LINES = 100 #testing                 
LOCAL_LOG_FILE = "test.log"     
HDFS_DEST_PATH = "/home/rami/logs/test.log"  

def now_iso(): # date
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
def random_invoice(): # trans_id
    return str(random.randint(100000, 999999))
def random_ipv4():
    return ".".join(str(random.randint(1, 254)) for _ in range(4))
def random_status():
    return random.choice([200, 201, 400, 401, 403, 404, 500, 502, 503])
def generate_log_file(filepath: Path, lines: int):
    with filepath.open("w", encoding="utf-8") as f:
        for _ in range(lines):
            line = (
                f"{now_iso()} | "   # timestamp
                f"Invoice={random_invoice()} | " # tran_id
                f"IP={random_ipv4()} | " # server_ip
                f"Status={random_status()}\n" # sc
            )
            f.write(line)
    print(f"Generated log file: {filepath}")

def ensure_hdfs_directory(hdfs_file_path: str):
    dir_path = os.path.dirname(hdfs_file_path)

    print(f"Checking if HDFS directory exists: {dir_path}")
    check_cmd = ["hdfs", "dfs", "-test", "-d", dir_path]
    result = subprocess.call(check_cmd)

    if result == 0:
        print("HDFS directory already exists.")
        return True

    print("Directory does NOT exist. Creating it...")
    mkdir_cmd = ["hdfs", "dfs", "-mkdir", "-p", dir_path]

    try:
        subprocess.run(mkdir_cmd, check=True)
        print("HDFS directory created successfully.")
        return True
    except Exception as e:
        print("Failed to create HDFS directory:", e)
        return False


def upload_to_hdfs(local: Path, dest: str):
    if not ensure_hdfs_directory(dest):
        print("Cannot proceed without HDFS directory.")
        return False

    print(f"Uploading {local} → {dest}")

    try:
        cmd = ["hdfs", "dfs", "-put", "-f", str(local), dest]
        subprocess.run(cmd, check=True)
        print("Upload successful (hdfs dfs -put)")
        return True
    except Exception as e:
        print("HDFS upload failed:", e)
        return False


def main():
    local_path = Path(LOCAL_LOG_FILE)
    generate_log_file(local_path, NUMBER_OF_LINES)
    success = upload_to_hdfs(local_path, HDFS_DEST_PATH)
    if not success:
        print("Upload failed. File remains locally.")
if __name__ == "__main__":
    main()