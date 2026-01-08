from typing import Dict
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema
from utils.logger import get_logger

logger = get_logger(__name__)


def ensure_topic(bootstrap_servers: str, topic: str, config: Dict):
    """Create topic if it doesn't exist."""
    admin = AdminClient({'bootstrap.servers': bootstrap_servers})
    topics = admin.list_topics().topics
    if topic not in topics:
        logger.info(f"Topic {topic} not found → creating...")
        new_topic = NewTopic(
            topic,
            num_partitions=config["topics"][topic]["partitions"],
            replication_factor=config["topics"][topic]["replication_factor"]
        )
        fs = admin.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f"Topic {topic} created...")
            except Exception as e:
                logger.error(f"Failed to create {topic}: {e}...")
    else:
        logger.info(f"Topic {topic} already exists...")


def register_schema(sr_client: SchemaRegistryClient, topic: str, schema_str: str):
    """Register Avro schema if not already present."""
    subject = f"{topic}-value"
    try:
        registered = sr_client.get_latest_version(subject)
        if registered.schema.schema_str == schema_str:
            logger.info(f"Schema already registered for {subject} (ID: {registered.id})...")
            return registered.id
    except Exception:
        pass  # Not registered

    logger.info(f"Registering new schema for {subject}...")
    schema_id = sr_client.register_schema(subject, Schema(schema_str))
    logger.info(f"Schema registered → ID: {schema_id}...")
    return schema_id


def delivery_report(err, msg):
    """Delivery callback."""
    if err is not None:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")