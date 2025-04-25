
from confluent_kafka.admin import AdminClient, NewTopic

admin_client = AdminClient({
    "bootstrap.servers": "b-1.kk.vkp9vv.c18.kafka.us-east-1.amazonaws.com:9096,b-2.kk.vkp9vv.c18.kafka.us-east-1.amazonaws.com:9096,b-3.kk.vkp9vv.c18.kafka.us-east-1.amazonaws.com:9096",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "username",
    "sasl.password": "password",
})

# Define multiple topics
topics_to_create = [
    NewTopic("cds-task-executor-evaluation-ci", num_partitions=3, replication_factor=2),
    NewTopic("cds-task-executor-remediation-ci", num_partitions=2, replication_factor=2),
    NewTopic("cds-task-executor-findings-ci", num_partitions=3, replication_factor=2),
    NewTopic("cds-task-executor-evaluation-stage", num_partitions=3, replication_factor=2),
    NewTopic("cds-task-executor-remediation-stage", num_partitions=2, replication_factor=2),
    NewTopic("cds-task-executor-findings-stage", num_partitions=3, replication_factor=2)
]

# Create topics
fs = admin_client.create_topics(topics_to_create)

# Wait for each to finish
for topic, f in fs.items():
    try:
        f.result()  # Will raise exception if topic creation failed
        print(f"✅ Topic '{topic}' created")
    except Exception as e:
        print(f"❌ Failed to create topic '{topic}': {e}")

