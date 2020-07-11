import datetime
import json
from concurrent.futures import ThreadPoolExecutor
import pickle
import requests
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.client_async import KafkaClient
from kafka import KafkaConsumer, KafkaProducer


KAFKA_SERVER = 'kafka:9092'
KAFKA_HIGHLIGHT_WORKER = 'hbot.core.highlight-worker'
KAFKA_OUTGOING_TOPIC = 'hbot.hooks.fanout'


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


# Given clip context and the goal scored timestamp,
# find the clips to download and stitch the clips together to form one video.
#
# Defaults to a 20 second clip
#
# Returns the bytes to the highlight clip
def build_highlight(clip_context_metadata: list, goal_event_timestamp: int) -> bytes:
    # (Add padding to the goal timestamp)
    # TODO: Look more into this later
    goal_event_timestamp += 0

    # Find the last 20 seconds since the goal was scored
    goal_event_timestamp_start = goal_event_timestamp - 20
    goal_event_timestamp_end = goal_event_timestamp

    # Get clips from the context that match the timestamps (20 second clip)
    clips = list(filter(lambda metadata: metadata['timestamp'] >= goal_event_timestamp_start and\
                                         metadata['timestamp'] <= goal_event_timestamp_end,
                        clip_context_metadata))

    # Download the clips and reverse (so the oldest clip is first in the video)
    clips_data = download_clips(clips)
    clips_data = list(reversed(list(map(lambda x: x[1], clips_data))))
    print('  Downloaded clips:', len(clips_data))

    # Stitch together the clips into one video
    clips_combined = b''.join(clips_data)
    del clips_data

    return clips_combined


# Given a list of (timestamp, video_url), return the (timestamp, video buffer)
# by downloading each individual clip.
# Return the clips as a list in their timestamp order (descending)
def download_clips(clips_metadata: list):
    # Start a pool of executors to download the clip data concurrently
    with ThreadPoolExecutor(max_workers=8) as pool:
        # Download concurrently with a map call on the pool.
        # Calls the `download_clip` function n times for each of the n clips to download.
        result = list(pool.map(download_clip, clips_metadata))

        # Filter out any bad requests
        result = list(filter(lambda metadata: metadata[0] is not None, result))

        # Sort the responses by timestamp (descending) so the data can be pieced together
        # into a single video.
        result_sorted = list(sorted(result, key=lambda metadata: metadata[0], reverse=True))

        return result_sorted


# Download a single clip.
def download_clip(clip_metadata) -> (int, bytes):
    clip_timestamp, clip_url = clip_metadata['timestamp'], clip_metadata['uri']

    try:
        response = requests.get(clip_url)
        video_buffer = response.content # bytes for the video

        print(f'  Downloaded clip {clip_timestamp}')

        return clip_timestamp, video_buffer
    except:
        print(f'  Couldn\'t download clip {clip_timestamp}')
        return None, None


# Save a highlight to disk
def save_highlight(highlight_video_buffer: bytes, timestamp: int) -> None:
    with open(f'./clips/clip-{timestamp}.ts', 'wb') as handler:
        handler.write(highlight_video_buffer)
        handler.close()



# Correct the timezone for a timestamp.
# For some reason, Twitch likes to use unix timestamps that are already in UTC,
# therefore, move forward the goal event timestamps to match.
# 
# Input: the original unix timestamp and the original timezone location.
#
# Output: Unix timestamp with the UTC location
def get_corrected_timezone(original_timestamp: int, original_timezone: str):
    # TODO: Do this properly with pytz
    return original_timestamp + 14400


if __name__ == '__main__':
    print('Starting video worker consumer')
    setup(KAFKA_HIGHLIGHT_WORKER)
    setup(KAFKA_OUTGOING_TOPIC)

    kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                                   api_version=(2, 5, 0))

    kafka_consumer = KafkaConsumer(KAFKA_HIGHLIGHT_WORKER,
                                   bootstrap_servers=KAFKA_SERVER,
                                   api_version=(2, 5, 0),
                                   group_id='highlight_worker_group',
                                   auto_offset_reset='earliest',
                                   enable_auto_commit=True)

    try:
        # Wait for incoming Highlight Video work
        # When a work message comes in, download the pieces to the highlight video
        # and upload to whatever service we want.
        for message in kafka_consumer:
            payload_bytes = message.value

            # payload = pickle_rick.loads(payload_bytes) or pickle_rick.dumps(bytes(dict()))
            # Decode and deserialize the payload
            payload = payload_bytes.decode('utf-8').replace("'", '"')
            payload = json.loads(payload)
            print('Incoming event:', payload)

            clip_context_metadata = payload['videoClip']
            goal_event_timestamp = payload['goalEventTimestamp']

            # Convert timestamp to a human readable format
            timestamp = goal_event_timestamp#- 14400 # Hacky fix for Twitch's timestamps

            # Build the highlight
            highlight_video_buffer = build_highlight(clip_context_metadata, goal_event_timestamp)
            save_highlight(highlight_video_buffer, timestamp)
            
            print('Highlight saved!')

            del highlight_video_buffer

            # Send message to Hooks
            payload = dict(timestamp=timestamp)
            payload_bytes = pickle.dumps(payload) # TODO: replace with protobuf
            kafka_producer.send(KAFKA_OUTGOING_TOPIC, payload_bytes)

    except KeyboardInterrupt:
        kafka_consumer.close()
        kafka_producer.close()
        print('interrupted')
