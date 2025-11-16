#!/usr/bin/env python3
import random
from datetime import datetime, timezone
from pathlib import Path
import os
import time

# flume.conf
NUMBER_OF_LINES = 50                   
SPOOL_DIR = "/spooldir"                
BASENAME = "events"                    


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
def random_invoice():
    return str(random.randint(100000, 999999))
def random_ipv4():
    return ".".join(str(random.randint(1, 254)) for _ in range(4))
def random_status():
    return random.choice([200, 201, 400, 401, 403, 404, 500, 502, 503])
def generate_flume_file():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    final_filename = f"{BASENAME}-{timestamp}.log"
    temp_filename = f".{final_filename}.tmp" # rename file as the conf needs

    spool_path = Path(SPOOL_DIR)
    temp_path = spool_path / temp_filename
    final_path = spool_path / final_filename

    print(f"Generating log file: {final_path}")

    # Write to temp file (Flume ignores files starting with ".")
    with temp_path.open("w", encoding="utf-8") as f:
        for _ in range(NUMBER_OF_LINES):
            line = (
                f"{now_iso()} | "
                f"Invoice={random_invoice()} | "
                f"IP={random_ipv4()} | "
                f"Status={random_status()}\n"
            )
            f.write(line)

    # flush
    time.sleep(0.1)

    # Rename to final name → Flume picks it up
    temp_path.rename(final_path)

    print(f"File ready for Flume ingestion: {final_path}")


def main():
    if not os.path.exists(SPOOL_DIR):
        os.makedirs(SPOOL_DIR)
        print(f"Created spool directory: {SPOOL_DIR}")
    generate_flume_file()


if __name__ == "__main__":
    main()