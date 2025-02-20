'''
csv_producer_kilchenmann.py

Stream CSV data to a Kafka topic.

It is common to transfer CSV data as JSON so 
each field is clearly labeled. 
'''

#####################################
# Import Modules
#####################################

import os
import sys
import time
import pathlib
import csv
import json
from datetime import datetime
from dotenv import load_dotenv
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("SMOKER_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval

#####################################
# Set up Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

DATA_FOLDER = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

DATA_FILE = DATA_FOLDER.joinpath("historical_actors_names.csv")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Message Generator
#####################################

def generate_messages(file_path: pathlib.Path):
    """
    Read from a CSV file and yield records one by one, continuously.

    Args:
        file_path (pathlib.Path): Path to the CSV file.

    Yields:
        dict: CSV row formatted as a dictionary.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {DATA_FILE}")
            with open(DATA_FILE, "r") as csv_file:
                logger.info(f"Reading data from file: {DATA_FILE}")

                csv_reader = csv.DictReader(csv_file)
                csv_reader.fieldnames = [field.strip().lower() for field in csv_reader.fieldnames]
                for row in csv_reader:
                    row = {key.lower(): value for key, value in row.items()}  # Normalize column names
                    
                    # Ensure required fields are present
                    if "name" not in row or "course" not in row or "weekday" not in row:
                        logger.error(f"Missing required column in row: {row}")
                        continue

                    # Generate a timestamp and prepare the message
                    current_timestamp = datetime.utcnow().isoformat()
                    message = {
                        "name": row["name"],
                        "course": row["course"],
                        "weekday": row["weekday"],
                        "timestamp": current_timestamp
                    }
                    logger.debug(f"Generated message: {message}")
                    yield message
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)

#####################################
# Define main function for this module.
#####################################

def main():
    """
    Main entry point for the producer.
    """

    logger.info("START producer.")
    verify_services()

    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for csv_message in generate_messages(DATA_FILE):
            producer.send(topic, value=csv_message)
            logger.info(f"Sent message to topic '{topic}': {csv_message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END producer.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()