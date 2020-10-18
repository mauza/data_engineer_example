import os
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()
kafka_broker = os.getenv('kafka_broker')

admin_client = AdminClient({
    "bootstrap.servers": kafka_broker
})

topic_list = []
topic_list.append(NewTopic("example_topic", 1, 1))
admin_client.create_topics(topic_list)