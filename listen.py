from confluent_kafka import Consumer
import json
import logging


# Kafka configuration

KAFKA_TOPIC = 'cds-task-executer-findings'  # Replace with your topic


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka consumer configuration
consumer_config = {
    'group.id': 'evaluator_status_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    "bootstrap.servers": "b-1.kk.vkp9vv.c18.kafka.us-east-1.amazonaws.com:9096,b-2.kk.vkp9vv.c18.kafka.us-east-1.amazonaws.com:9096,b-3.kk.vkp9vv.c18.kafka.us-east-1.amazonaws.com:9096",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    'sasl.username': 'username',
    'sasl.password': 'password',
}

def list_messages(topic, max_messages=10):
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])

    messages = []
    logger.info(f"📥 Consuming messages from topic: {topic}")

    try:
        while len(messages) < max_messages:
            msg = consumer.poll(1.0)  # 1 second timeout
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Error: {msg.error()}")
                continue

            try:
                decoded_value = msg.value().decode('utf-8')
                data = json.loads(decoded_value)
                messages.append(data)
                logger.info(f"✅ Received message: {data}")
            except Exception as e:
                logger.error(f"Error decoding message: {e}")

    except KeyboardInterrupt:
        logger.info("🛑 Stopping consumer.")
    finally:
        consumer.close()

    return messages

if __name__ == "__main__":
    msgs = list_messages(KAFKA_TOPIC, max_messages=10)
    print("\n🎯 Final message list:")
    for m in msgs:
        print(json.dumps(m, indent=2))
