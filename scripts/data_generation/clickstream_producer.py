"""
Kafka Clickstream Producer – generates real-time user events
"""
import time
import uuid
from utils.logger import get_logger
from scripts.data_generation.base.kafka_producer import KafkaGenericProducer

logger = get_logger("ClickStreamDataGenerator")

class ClickStreamDataGenerator(KafkaGenericProducer):
    """Generates and sends real-time clickstream events to Kafka."""
    SCHEMA_PATH = "configs/data_generation/kafka/schemas/clickstream.avsc"

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


if __name__ == "__main__":
    clickstream_topic = "clickstream"
    clickstream_generator = ClickStreamDataGenerator(clickstream_topic)
    clickstream_generator.run()