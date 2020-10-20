import os
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()
kafka_broker = os.getenv('kafka_broker')
topics = os.getenv('topics').split(',')

a = AdminClient({'bootstrap.servers': kafka_broker})

fs = a.delete_topics(topics, operation_timeout=30)

# Wait for operation to finish.
for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print("Topic {} deleted".format(topic))
    except Exception as e:
        print("Failed to delete topic {}: {}".format(topic, e))