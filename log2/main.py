import logging
from ingestion import read_log
from transform import transform_record
from load import load_batch

logging.basicConfig(level=logging.INFO)

def run_pipeline(file_path, table_id, batch_size=1000):
    batch = []

    for record in read_log(file_path):
        try:
            transformed = transform_record(record)
            batch.append(transformed)

            if len(batch) >= batch_size:
                load_batch(batch, table_id)
                logging.info(f"Loaded batch of {batch_size}")
                batch = []

        except Exception as e:
            logging.error(f"Error processing record: {e}")

    if batch:
        load_batch(batch, table_id)
        logging.info("Loaded final batch")


if __name__ == "__main__":
    run_pipeline("./crown_interactive_january_logs.json","dataintershipprogram.LogParser.mongo_log")
