import csv
from datetime import datetime, timedelta

csv_file = 'ecommerce_kaggle_sample.csv'
log_file = 'ecommerce.log'

# Date initiale fictive pour horodatage
base_time = datetime(2025, 11, 11, 9, 0, 0)
time_delta = timedelta(seconds=3)

with open(csv_file, newline='', encoding='utf-8') as f_csv, open(log_file, 'w', encoding='utf-8') as f_log:
    reader = csv.DictReader(f_csv)
    for i, row in enumerate(reader):
        timestamp = (base_time + i * time_delta).strftime('%Y-%m-%d %H:%M:%S')
        log_line = f'{timestamp} INFO Product Sold: {dict(row)}\n'
        f_log.write(log_line)

print("Fichier log généré avec succès :", log_file)
