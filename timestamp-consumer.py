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


# Save new (timestamp, video_url) tuples in redis cache
# Remove old urls from the redis cache.
#
# Pickles the data to save in redis.
#
# Input:
#     new_video_urls: list of tuples [(timestamp, video_url), ...]
#     old_video_timestamps: list of timestamps (int)
# Output: None
def update_video_urls(new_video_urls: list, old_video_timestamps: list):
    pass


# Return timestamp of the first video in video metadata
def get_head(video_metadata: list) -> int:
    return video_metadata[0][0]


# Returns the current context of the video clips
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
    current_video_metadata = pickle_rick.loads(r.get(REDIS_HBOT_VIDEO_METADATA) or bytes([]))

    if current_video_metadata == []:
        # No clip metadata stored! Cache all initial clip metadata now for the next iteration.
        payload_data_bytes = pickle_rick.dumps(payload_data)
        r.set(REDIS_HBOT_VIDEO_METADATA, payload_data_bytes)

        # Find initial head and tail
        head, tail = current_video_metadata[0][0], current_video_metadata[-1][0]

        return current_video_metadata, head, tail

    # Else: There is clip metadata stored
    # Note: Store a maxium of 60 video timestamps
    # Find the difference between the incoming set and current set of timestamps
    # The difference should be the new video metadata to add

    # First, convert the current metadata into a set of timestamps
    current_video_metadata_set = set(map(lambda metadata: metadata[0], current_video_metadata))

    # Find the new video clip metadata by comparing timestamps, these are
    # the new individual video metadata information to cache.
    new_video_metadata = list(filter(lambda metadata: metadata[0] not in current_video_metadata_set), payload_data)

    # Merge together the current (old) metadata and new metadata
    updated_video_metadata = new_video_metadata + payload_data

    # Sort by timestamp desc, then take only the first 60 items (approx 120 seconds in total)

    # Update the head and 

    # current_video_timestamps = set(map(lambda timestamp: int(timestamp), current_video_timestamps))
    # print('Current timestamps', len(current_video_timestamps), current_video_timestamps)

    return [], None, None


# Return the new videos, old videos (too old to keep), the head (first timestamp)
# of the current stream, and the tail (last timestamp) of the current stream
def work(r, payload_timestamp, payload_sequence, payload_data):
    # Check if the sequence number has changed
    current_sequence = r.get(REDIS_HBOT_SEQUENCE)
    if current_sequence is not None and payload_sequence == int(current_sequence):
        # Nothing has changed
        return [], [], None, None

    # Save last 60 video clips (2 seconds each)
    # Check which timestamps are new by finding the differences in sets
    current_video_timestamps = set(r.smembers(REDIS_HBOT_VIDEO_METADATA) or set())
    current_video_timestamps = set(map(lambda timestamp: int(timestamp), current_video_timestamps))
    # print('Current timestamps', len(current_video_timestamps), current_video_timestamps)

    # List of incoming video timestamps
    incoming_video_timestamp_set = set(map(lambda item_tuple: item_tuple[0], payload_data))

    # Set new sequence number + timestamp
    r.set(REDIS_HBOT_SEQUENCE, payload_sequence)
    r.set(REDIS_HBOT_TIMESTAMP, payload_timestamp)

    if current_video_timestamps is None:
        # No clips stored, add all clip timestamps
        r.set(REDIS_HBOT_VIDEO_METADATA, *incoming_video_timestamp_set)

        head, tail = max(current_video_timestamps), min(current_video_timestamps)

        # Return the timestamps and corresponding video urls to all of the clips
        return payload_data, [], head, tail

    # Else:
    # Note: Store a maxium of 60 video timestamps
    # Find the difference between the incoming set and current set of timestamps
    # The difference should be the new video timestamps to add
    difference = incoming_video_timestamp_set - current_video_timestamps
    new_timestamps = list(current_video_timestamps.union(difference))

    # Sort new video timestamps by order descending and
    # keep only the latest 60 timestamps
    new_timestamps_sorted = list(reversed(sorted(new_timestamps)))
    old_timestamps_list = new_timestamps_sorted[60:] # clips too old to keep
    new_timestamps_list = new_timestamps_sorted[0:60] # new clips and old clips to keep
    new_timestamps_set = set(new_timestamps_list)

    # Update head (first timestamp) and tail (last timestamp), so we know where the current video is
    head, tail = new_timestamps_list[0], new_timestamps_list[-1]
    r.set(REDIS_HBOT_VIDEO_HEAD, head)
    r.set(REDIS_HBOT_VIDEO_TAIL, tail)

    # print('New timestamps:', len(new_timestamps_set), new_timestamps_set)
    r.delete(REDIS_HBOT_VIDEO_METADATA)
    r.sadd(REDIS_HBOT_VIDEO_METADATA, *new_timestamps_set)

    # Get the corresponding video urls for the newly added timestamps (not all timestamps)
    new_payload_data_list = list(filter(lambda item_tuple: item_tuple[0] in difference, payload_data))

    return new_payload_data_list, old_timestamps_list, head, tail


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
            new_video_data, old_video_data, head, tail = work(r, payload_timestamp, payload_sequence, payload_data)

            # Build, pickle and send
            timestamp_range_payload_data = dict(head=head, tail=tail)

            timestamp_range_payload_data = pickle_rick.dumps(timestamp_range_payload_data)
            kafka_producer.send(KAFKA_OUTGOING_TOPIC, timestamp_range_payload_data)

            # print('New video data:')
            # print(len(new_video_data), new_video_data, '\n-----\n')
    except KeyboardInterrupt:
        kafka_consumer.close()
        kafka_producer.close()
        print('interrupted')
