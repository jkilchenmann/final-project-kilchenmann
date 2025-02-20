'''
csv_consumer_kilchenmann.py

Consume JSON messages from a Kafka topic and generate a real-time histogram.
'''

#####################################
# Import Modules
#####################################

import os
import json
import matplotlib.pyplot as plt
import collections
import numpy as np

from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
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

def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

#####################################
# Message Processing and Histogram Generation
#####################################

course_counts = collections.defaultdict(lambda: collections.defaultdict(int))

plt.ion()  # Enable interactive mode for real-time updates
fig, ax = plt.subplots(figsize=(10, 6))

day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

def process_messages(consumer):
    """Process messages from Kafka and update histogram data in real-time."""
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            
            try:
                data = json.loads(message_str)
                course = data.get("course")
                weekday = data.get("weekday")
                
                if not course or not weekday:
                    logger.error(f"Invalid message format: {data}")
                    continue
                
                course_counts[course][weekday] += 1
                update_histogram()
            except json.JSONDecodeError as e:
                logger.error(f"JSON decoding error: {e}")
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
        # Optionally, add logic to attempt reconnect or cleanup
    finally:
        try:
            consumer.close()
            logger.info("Kafka consumer closed.")
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")

def update_histogram():
    """Update the histogram with course visit frequencies without creating a new figure."""
    ax.clear()
    
    courses = list(course_counts.keys())
    if not courses:
        logger.warning("No data available for plotting yet.")
        return
    
    days = day_order
    x = np.arange(len(days))  # X-axis positions
    width = 0.15  # Width of each bar
    
    for i, course in enumerate(courses):
        counts = [course_counts[course].get(day, 0) for day in days]
        ax.bar(x + i * width, counts, width=width, label=course)
    
    ax.set_xlabel("Weekday")
    ax.set_ylabel("Number of Visits")
    ax.set_title("Course Attendance by Weekday")
    ax.legend()
    ax.set_xticks(x + (len(courses) - 1) * width / 2)
    ax.set_xticklabels(days, rotation=45)
    plt.draw()
    plt.pause(0.1)  # Update plot without blocking execution

def main():
    """Main entry point for the consumer."""
    logger.info("START consumer.")
    
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    
    consumer = create_kafka_consumer(topic, group_id)
    logger.info(f"Polling messages from topic '{topic}'...")
    
    process_messages(consumer)

if __name__ == "__main__":
    main()