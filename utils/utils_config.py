'''
Config Utility
File: utils/utils_config.py

This script provides the configuration functions for the project. 
It centralizes the configuration management 
by loading environment variables from .env in the root project folder
and constructing file paths using pathlib.
'''

#####################################
# Imports
#####################################

# import from Python Standard Library
import os
import pathlib

# import from external packages
from dotenv import load_dotenv

# import from local modules
from .utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_zookeeper_address() -> str:
    """Fetch ZOOKEEPER_ADDRESS from environment or use default."""
    address = os.getenv("ZOOKEEPER_ADDRESS", "localhost:2181")
    logger.info(f"ZOOKEEPER_ADDRESS: {address}")
    return address


def get_kafka_broker_address() -> str:
    """Fetch KAFKA_BROKER_ADDRESS from environment or use default."""
    address = os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092")
    logger.info(f"KAFKA_BROKER_ADDRESS: {address}")
    return address


def get_kafka_topic() -> str:
    """Fetch KAFKA_TOPIC from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "tutor_topic")
    logger.info(f"KAFKA_TOPIC: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch TUTOR_INTERVAL_SECONDS from environment or use default."""
    interval = int(os.getenv("TUTOR_INTERVAL_SECONDS", 1))
    logger.info(f"TUTOR_INTERVAL_SECONDS: {interval}")
    return interval


def get_kafka_consumer_group_id() -> str:
    """Fetch TUTOR_CONSUMER_GROUP_ID from environment or use default."""
    group_id = os.getenv("TUTOR_CONSUMER_GROUP_ID", "tutor_group")
    logger.info(f"TUTOR_CONSUMER_GROUP_ID: {group_id}")
    return group_id


def get_base_data_path() -> pathlib.Path:
    """Fetch BASE_DATA_DIR from environment or use default."""
    project_root = pathlib.Path(__file__).parent.parent
    data_dir = project_root / os.getenv("BASE_DATA_DIR", "data")
    logger.info(f"BASE_DATA_DIR: {data_dir}")
    return data_dir