
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer


def retrieve_key_from_url(url):
    if 'jk=' in url:
        return url.split('jk=')[1].split('&')[0]
    elif 'fccid=' in url:
        return url.split('fccid=')[1].split('&')[0]
    else:
        return url


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for Job Url record {}: {}".format(msg.key(), err))
        return
    print('Job Url record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def publish_job_url_to_kafka(url, kafka_topic, kafka_producer):
    key = retrieve_key_from_url(url)
    print(f'publishing url: {url}')
    kafka_producer.produce(topic=kafka_topic, key=key, value=url, on_delivery=delivery_report)


def url_to_dict(url, ctx):
    return {'url': url}


def generate_kafka_producer(kafka_broker, schema_registry, schema_str):
    schema_registry_conf = {'url': schema_registry}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    json_serializer = JSONSerializer(schema_str, schema_registry_client, url_to_dict)

    producer_conf = {'bootstrap.servers': kafka_broker,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': json_serializer}

    return SerializingProducer(producer_conf)


def dict_to_url(obj, ctx):
    return obj['url']


def generate_kafka_consumer(kafka_broker, schema_str, group):
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=dict_to_url)
    string_deserializer = StringDeserializer('utf_8')

    consumer_conf = {'bootstrap.servers': kafka_broker,
                     'key.deserializer': string_deserializer,
                     'value.deserializer': json_deserializer,
                     'group.id': group,
                     'auto.offset.reset': "earliest"}

    return DeserializingConsumer(consumer_conf)
