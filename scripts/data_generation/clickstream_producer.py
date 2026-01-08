"""
Kafka Clickstream Producer – generates real-time user events
"""
import sys
import json
import time
import uuid
from pathlib import Path
from datetime import datetime
from avro.schema import parse
from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from utils.config import get_project_root, load_config
from utils.logger import get_logger
from utils.kafka import ensure_topic, register_schema, delivery_report

logger = get_logger("ClickStreamDataGenerator")

class ClickStreamDataGenerator:
    """Generates and sends real-time clickstream events to Kafka."""
    CONFIG_PATH = "configs/data_generation/kafka/topics.yaml"
    SCHEMA_PATH = "configs/data_generation/kafka/schemas/clickstream.avsc"

    def __init__(self, config=None, schema=None):
        if config is None:
            config = dict()
        self.config = {**config}  # in-case user provides new configs
        default_config = load_config(self.CONFIG_PATH).get("kafka", {})
        self.config.update(default_config)  # We'll use default_config(what we have in configs)
        self.topic = "clickstream"
        self.bootstrap_servers = self.config.get("bootstrap_servers", "kafka:9092")
        self.sr_url = "http://schema-registry:8081"

        ensure_topic(self.bootstrap_servers, self.topic, self.config)
        self.sr_client = SchemaRegistryClient({'url': self.sr_url})

        project_root = get_project_root()
        full_schema_path = (project_root/self.SCHEMA_PATH).resolve()

        with open(full_schema_path, "r") as f:
            self.schema_str = f.read()

        self.schema_id = register_schema(self.sr_client, self.topic, self.schema_str)
        self.avro_serializer = AvroSerializer(self.sr_client, self.schema_str)
        self.producer = Producer({
            'bootstrap.servers': self.bootstrap_servers,
            'acks': 'all'
        })
        logger.info(f"ClickstreamProducer ready → topic: {self.topic}, schema ID: {self.schema_id}...")

    def generate_event(self):
        """Generate one realistic clickstream event."""
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": f"USER_{uuid.uuid4().hex[:8]}",
            "session_id": str(uuid.uuid4()),
            "page_url": f"https://retail360.com/{['home', 'product', 'cart', 'checkout'][uuid.uuid4().int % 4]}",
            "event_type": ["view", "click", "add_to_cart", "purchase"][uuid.uuid4().int % 4],
            "event_time": int(time.time() * 1000),  # millis since epoch
            "device_type": ["mobile", "desktop", "tablet"][uuid.uuid4().int % 3]
        }

    def run(self):
        """Produce events continuously."""
        interval = 1.0 / self.config["topics"][self.topic]["messages_per_second"]
        start = time.time()
        while time.time() - start < self.config["topics"][self.topic]["run_duration_seconds"]:
            event = self.generate_event()
            try:
                serialized = self.avro_serializer(event, SerializationContext(self.topic, MessageField.VALUE))
                self.producer.produce(
                    topic=self.topic,
                    value=serialized,
                    callback=delivery_report
                )
                logger.info(f"Sent event: {event['event_id']}...")
            except Exception as e:
                logger.error(f"Failed to send: {e}...")
            time.sleep(interval)

        self.producer.flush()
        logger.info("Producer finished.")


if __name__ == "__main__":
    clickstream_generator = ClickStreamDataGenerator()
    clickstream_generator.run()