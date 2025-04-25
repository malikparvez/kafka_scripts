from confluent_kafka.admin import AdminClient

admin_client = AdminClient({
    "bootstrap.servers": "b-1.kk.vkp9vv.c18.kafka.us-east-1.amazonaws.com:9096,b-2.kk.vkp9vv.c18.kafka.us-east-1.amazonaws.com:9096,b-3.kk.vkp9vv.c18.kafka.us-east-1.amazonaws.com:9096",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "username",
    "sasl.password": "password",
})

# List of topic names to delete
topics_to_delete = [
  "cds-task-executer-findings-ci",
  "cds-task-executer-remediation-stage",
  "cds-task-executer-findings-stage",
  "cds-task-executor-evaluation-ci",
  "cds-task-executor-evaluation-stage",
  "cds-task-executer-remediation-ci",
  "evaluation_task_status"
]

# Trigger delete request
fs = admin_client.delete_topics(topics_to_delete, operation_timeout=30)

# Wait for each deletion to complete
for topic, f in fs.items():
    try:
        f.result()  # Raises exception if deletion failed
        print(f"🗑️  Topic '{topic}' deleted successfully")
    except Exception as e:
        print(f"❌ Failed to delete topic '{topic}': {e}")
