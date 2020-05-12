import requests
import pickle as pickle_rick
import redis
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.client_async import KafkaClient
from kafka import KafkaConsumer


KAFKA_SERVER = '127.0.0.1:9092'
KAFKA_TOPIC_VIDEO_CLIPS = 'hbot.worker.video-clips'
KAFKA_TOPIC_GOAL_EVENTS = 'hbot.worker.goal-events'
KAFKA_TOPICS = [KAFKA_TOPIC_VIDEO_CLIPS, KAFKA_TOPIC_GOAL_EVENTS]
REDIS_PREFIX = 'hbot'
REDIS_HBOT_VIDEO_TIMESTAMPS = f'{REDIS_PREFIX}-video-timestamps'


# Update head and tail timestamp range.
# Returns the new timestamp range.
# def update_range(current_head, current_tail, new_head, new_tail):
#     head, tail = current_head, current_tail

#     if current_head is None or new_head > head:
#         head = new_head

#     if current_tail is None or new_tail > tail:
#         tail = new_tail

#     return head, tail


def setup(topic_name):
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


if __name__ == '__main__':
    print('Starting video worker consumer')
    
    for topic in KAFKA_TOPICS:
        setup(topic)

    r = redis.Redis(host='127.0.0.1', port=6379, db=0)

    kafka_consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER,
                                   api_version=(2, 5, 0),
                                   group_id='stream_video_timestamp_range',
                                   auto_offset_reset='earliest',
                                   enable_auto_commit=True)
    kafka_consumer.subscribe(topics=tuple(KAFKA_TOPICS))

    try:
        # Head and tail of the currently available video clips urls in redis
        head, tail = None, None

        # Wait for messages
        for message in kafka_consumer:
            message_topic = message.topic
            payload_bytes = message.value

            payload = pickle_rick.loads(payload_bytes)

            if message_topic == KAFKA_TOPIC_VIDEO_CLIPS:
                print('Got video clips message (got newest set of video clips)')
                # New head (first timestamp)
                new_head = payload['head']

                # New tail (last timestamp)
                new_tail = payload['tail']

                # Update head and tail range based on 
                head, tail = update_range(head, tail, new_head, new_tail)
                
                # Save the new head and tail
                print('head:', new_head, ' tail:', new_tail)
                print('----\n')
            
            if message_topic == KAFKA_TOPIC_GOAL_EVENTS:
                print('Got goal events range message')
                # Take the match set of video clips as the context for the goal event,
                # then process the 

    except KeyboardInterrupt:
        kafka_consumer.close()
        print('interrupted')
