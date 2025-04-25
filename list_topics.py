
# List all topics
from confluent_kafka.admin import AdminClient

# Configuration

admin_config = {
    "bootstrap.servers": "b-1.kk.vkp9vv.c18.kafka.us-east-1.amazonaws.com:9096,b-2.kk.vkp9vv.c18.kafka.us-east-1.amazonaws.com:9096,b-3.kk.vkp9vv.c18.kafka.us-east-1.amazonaws.com:9096",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    'sasl.username': 'username',
    'sasl.password': 'password',
}

# Create Admin client
admin_client = AdminClient(admin_config)

print("🔍 Fetching topic list from Kafka broker...")
try:
    metadata = admin_client.list_topics(timeout=10)
    topics = metadata.topics
    print(f"✅ Found {len(topics)} topic(s):")
    for topic_name in topics:
        print(f" - {topic_name}")
except Exception as e:
    print(f"❌ Failed to list topics: {e}")

