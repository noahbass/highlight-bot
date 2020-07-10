import time
import json
import pickle
import ciso8601
import streamlink
import requests
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.client_async import KafkaClient
from kafka import KafkaProducer


KAFKA_SERVER = 'kafka:9092'


def setup(topic_name: str):
    # First, check if the topic already exists in kafka
    kafka_client = KafkaClient(bootstrap_servers=KAFKA_SERVER,
                               api_version=(2, 5, 0))

    future = kafka_client.cluster.request_update()
    kafka_client.poll(future=future)

    metadata = kafka_client.cluster
    current_topics = metadata.topics()

    kafka_client.close()
    
    print('Active topics:', current_topics)

    if topic_name not in current_topics:
        print(f'Creating topic {topic_name}...')
        kafka_admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER,
                                              api_version=(2, 5, 0))
        
        topic_list = [NewTopic(name=topic_name,
                               num_partitions=1,
                               replication_factor=1)]
        kafka_admin_client.create_topics(new_topics=topic_list, validate_only=False)

        kafka_admin_client.close()
    else:
        print(f'Topic {topic_name} exists')


# Helper function to send off a goal event to the topic
def send_goal_event(timestamp: int, topic: str):
    # Hacky
    # TODO: Fix this
    # Create producer for the kafka topic do get ready to publish
    kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                                   api_version=(2, 5, 0))

    payload = dict(timestamp=timestamp)
    
    # JSON to bytearray
    payload_bytes = json.dumps(payload).encode('utf-8')

    print('Sending payload:', payload, payload_bytes)
    kafka_producer.send(topic, payload_bytes)

    kafka_producer.close()


if __name__ == '__main__':
    # Create a test message
    now = int(time.time()) # unix timestamp
    send_goal_event(now)
