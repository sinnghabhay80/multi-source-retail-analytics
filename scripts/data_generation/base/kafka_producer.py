"""
Kafka Generic Producer Class
"""
import time
from avro.schema import parse
from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from utils.config import get_project_root, load_config
from utils.logger import get_logger

logger = get_logger("KafakGenericProducer")

class KafkaGenericProducer:
    """Kafka Generic Producer Class."""
    CONFIG_PATH = "configs/data_generation/kafka/topics.yaml"
    SCHEMA_PATH = None

    def __init__(self, topic: str, config=None, schema=None):
        if config is None:
            config = dict()
        self.config = {**config}  # in-case user provides new configs
        default_config = load_config(self.CONFIG_PATH).get("kafka", {})
        self.config.update(default_config)  # We'll use default_config(what we have in configs)
        self.topic = topic
        self.bootstrap_servers = self.config.get("bootstrap_servers")
        self.sr_url = self.config.get("schema_registry_url")

        self._ensure_topic()
        self.sr_client = SchemaRegistryClient({'url': self.sr_url})

        project_root = get_project_root()
        full_schema_path = (project_root/self.SCHEMA_PATH).resolve()

        with open(full_schema_path, "r") as f:
            self.schema_str = f.read()

        self.schema_id = self._register_schema()
        self.avro_serializer = AvroSerializer(self.sr_client, self.schema_str)
        self.producer = Producer({
            'bootstrap.servers': self.bootstrap_servers,
            'acks': 'all'
        })
        logger.info(f"Producer ready → topic: {self.topic}, schema ID: {self.schema_id}...")

    def _ensure_topic(self):
        """Create topic if it doesn't exist."""
        admin = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        topics = admin.list_topics().topics
        if self.topic not in topics:
            logger.info(f"Topic {self.topic} not found → creating...")
            new_topic = NewTopic(
                self.topic,
                num_partitions=self.config["topics"][self.topic]["partitions"],
                replication_factor=self.config["topics"][self.topic]["replication_factor"]
            )
            fs = admin.create_topics([new_topic])
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info(f"Topic {topic} created...")
                except Exception as e:
                    logger.error(f"Failed to create {topic}: {e}...")
        else:
            logger.info(f"Topic {self.topic} already exists...")

    def _register_schema(self):
        """Register Avro schema if not already present."""
        subject = f"{self.topic}-value"
        try:
            registered = self.sr_client.get_latest_version(subject)
            if registered.schema.schema_str == self.schema_str:
                logger.info(f"Schema already registered for {subject} (ID: {registered.id})...")
                return registered.id
        except Exception:
            pass  # Not registered

        logger.info(f"Registering new schema for {subject}...")
        schema_id = self.sr_client.register_schema(subject, Schema(self.schema_str))
        logger.info(f"Schema registered → ID: {schema_id}...")
        return schema_id

    def _delivery_report(self, err, msg):
        """Delivery callback."""
        if err is not None:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")

    def generate_event(self):
        """Generate one realistic event."""
        pass

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
                    callback=self._delivery_report
                )
                logger.info(f"Sent event: {event['event_id']}...")
            except Exception as e:
                logger.error(f"Failed to send: {e}...")
            time.sleep(interval)

        self.producer.flush()
        logger.info("Producer finished.")