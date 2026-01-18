"""
Kafka Offers Producer – generates real-time offers
"""
import time
import uuid
from utils.logger import get_logger
from scripts.data_generation.base.kafka_producer import KafkaGenericProducer

logger = get_logger("OffersDataGenerator")

class OffersDataGenerator(KafkaGenericProducer):
    """Generates and sends real-time offers to Kafka."""
    SCHEMA_PATH = "configs/kafka/schemas/offers.avsc"

    def generate_event(self):
        """Generate one realistic offer event."""
        return {
            "offer_id": str(uuid.uuid4()),
            "user_id": f"USER_{uuid.uuid4().hex[:8]}",
            "promo_id": f"PROMO_{uuid.uuid4().int % 20 + 1:03d}",
            "discount_pct": round(float(uuid.uuid4().int % 40 + 10), 1),
            "offer_time": int(time.time() * 1000),
            "status": ["sent", "viewed", "accepted", "expired"][uuid.uuid4().int % 4]
        }


if __name__ == "__main__":
    offers_topic = "offers"
    offers_generator = OffersDataGenerator(offers_topic)
    offers_generator.run()