import pickle as pickle_rick
import redis
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.client_async import KafkaClient
from kafka import KafkaConsumer, KafkaProducer


KAFKA_SERVER = '127.0.0.1:9092'
KAFKA_TOPIC = 'stream-metadata'
KAFKA_OUTGOING_TOPIC = 'hbot.worker.video-clips'
REDIS_PREFIX = 'hbot'
REDIS_HBOT_SEQUENCE = f'{REDIS_PREFIX}-sequence'
REDIS_HBOT_TIMESTAMP = f'{REDIS_PREFIX}-timestamp'
REDIS_HBOT_VIDEO_METADATA = f'{REDIS_PREFIX}-video-timestamps'
REDIS_HBOT_VIDEO_HEAD = f'{REDIS_PREFIX}-video-head'
REDIS_HBOT_VIDEO_TAIL = f'{REDIS_PREFIX}-video-tail'


# Returns the current context of the video clips.
# 
# Output:
#   - list of tuples containing the latest video clips [(timestamp, video_url), ...]
#   - head position (first timestamp)
#   - tail position (last timestamp)
def new_work(r: redis.Redis, payload_timestamp: int, payload_sequence: int, payload_data: list) -> (list, int, int):
    # Check if the sequence number has changed
    current_sequence = r.get(REDIS_HBOT_SEQUENCE)
    if current_sequence is not None and payload_sequence == int(current_sequence):
        # Nothing has changed
        return [], None, None

    # Set new sequence number + timestamp
    r.set(REDIS_HBOT_SEQUENCE, payload_sequence)
    r.set(REDIS_HBOT_TIMESTAMP, payload_timestamp)

    # Save metadata for last 60 video clips (2 seconds each)
    current_video_metadata = list(pickle_rick.loads(r.get(REDIS_HBOT_VIDEO_METADATA) or\
                                                    pickle_rick.dumps(bytes([]))))

    if current_video_metadata == []:
        # No clip metadata stored! Cache all initial clip metadata now for the next iteration.
        payload_data_bytes = pickle_rick.dumps(payload_data)
        r.set(REDIS_HBOT_VIDEO_METADATA, payload_data_bytes)

        # Find initial head and tail
        head, tail = payload_data[0][0], payload_data[-1][0]

        return current_video_metadata, head, tail

    # Else: There is clip metadata stored
    # Note: Store a maxium of 60 video timestamps
    # Find the difference between the incoming set and current set of timestamps
    # The difference should be the new video metadata to add
    # First, convert the current metadata into a set of timestamps
    current_video_metadata_set = set(map(lambda metadata: metadata[0], current_video_metadata))

    # Find the new video clip metadata by comparing timestamps, these are
    # the new individual video metadata information to cache.
    new_video_metadata = list(filter(lambda metadata: metadata[0] not in current_video_metadata_set, payload_data))

    # Merge together the current (old) metadata and new metadata
    updated_video_metadata = new_video_metadata + current_video_metadata

    # Sort by timestamp desc, then take only the first 60 items (approx 120 seconds in total)
    updated_video_metadata = list(sorted(updated_video_metadata, key=lambda metadata: metadata[0], reverse=True))
    updated_video_metadata = updated_video_metadata[0:60]

    # Update the head and tail timestamps
    head, tail = updated_video_metadata[0][0], updated_video_metadata[-1][0]

    # Pickle and save in redis cache for the next iteration
    updated_video_metadata_bytes = pickle_rick.dumps(updated_video_metadata)
    r.set(REDIS_HBOT_VIDEO_METADATA, updated_video_metadata_bytes)

    return updated_video_metadata, head, tail


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
    print('Starting timestamp consumer')

    setup(KAFKA_TOPIC)
    setup(KAFKA_OUTGOING_TOPIC)

    r = redis.Redis(host='127.0.0.1', port=6379, db=0)

    kafka_consumer = KafkaConsumer(KAFKA_TOPIC,
                             bootstrap_servers=KAFKA_SERVER,
                             api_version=(2, 5, 0),
                             group_id='kafka_consumer_group',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)

    # Create producer for the kafka topic do get ready to publish
    kafka_producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092',
                                   api_version=(2, 5, 0))

    try:
        # Wait for messages
        for message in kafka_consumer:
            payload_bytes = message.value
            payload = pickle_rick.loads(payload_bytes)

            payload_timestamp = payload['timestamp']
            payload_sequence = payload['sequence']
            payload_data = payload['data']
            print('Got message:', payload_timestamp, payload_sequence)

            # Process the incoming payload m3u8 stream data,
            # return only the new video clips and the video clips to delete (too old to save)
            # new_video_data, old_video_data, head, tail = work(r, payload_timestamp, payload_sequence, payload_data)
            updated_video_metadata, head, tail = new_work(r, payload_timestamp, payload_sequence, payload_data)

            # Build, pickle and send to video clip service
            timestamp_range_payload_data = dict(metadata=updated_video_metadata,
                                                head=head,
                                                tail=tail)

            timestamp_range_payload_data = pickle_rick.dumps(timestamp_range_payload_data)
            kafka_producer.send(KAFKA_OUTGOING_TOPIC, timestamp_range_payload_data)

            # Debugging information
            print('')
            print(f'Head: {head}, Tail: {tail}')
            for timestamp, video_url in updated_video_metadata:
                print('  ', timestamp, video_url[-12:]) # Print end of video url for debugging
    except KeyboardInterrupt:
        kafka_consumer.close()
        kafka_producer.close()
        print('interrupted')
