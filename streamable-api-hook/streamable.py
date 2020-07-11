import os
import datetime
import json
from concurrent.futures import ThreadPoolExecutor
import pickle as pickle_rick
import requests
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.client_async import KafkaClient
from kafka import KafkaConsumer


KAFKA_SERVER = 'kafka:9092'
KAFKA_TOPIC = 'hbot.hooks.fanout'
KAFKA_GROUP_ID = 'streamable_api_hook' # unique identifier for this hook
STREAMABLE_USER = os.getenv('STREAMABLE_USER')
STREAMABLE_PASS = os.getenv('STREAMABLE_PASS')


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


# Read a highlight from disk
# Clips are located in a shared volume with the highlight worker and other hooks
def read_highlight(timestamp: int) -> bytes:
    with open(f'./clips/clip-{timestamp}.ts', 'rb') as handler:
        contents = handler.read()
        handler.close()
        return contents


# Upload highlight to Streamable
#
# Returns the url to the uploaded streamable video
def upload_highlight_streamable(title: str, highlight_video_buffer: bytes) -> str:
    print('Uploading to Streamable..')
    try:
        response = requests.post('https://api.streamable.com/upload',
                                files={ 'file': highlight_video_buffer },
                                auth=(STREAMABLE_USER, STREAMABLE_PASS),
                                data={ 'title': title })

        print('Uploaded to Streamable!', response.status_code)
        # print(json.dumps(response.json(), indent=4)) # Debug
        response_json = response.json()
        shortcode = response_json['shortcode']

        return f'https://streamable.com/{shortcode}'
    except Exception as e:
        print('Couldn\'t upload to Streamable :(', str(e))
        return None


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
    print('-----------------')
    print('-----------------')
    print('-----------------')
    print('STREAMABLE:', STREAMABLE_USER, STREAMABLE_PASS)
    print('-----------------')
    print('-----------------')
    print('-----------------')
    setup(KAFKA_TOPIC)

    kafka_consumer = KafkaConsumer(KAFKA_TOPIC,
                                   bootstrap_servers=KAFKA_SERVER,
                                   api_version=(2, 5, 0),
                                   group_id=KAFKA_GROUP_ID)

    try:
        # Wait for incoming Highlight Video work
        # When a work message comes in, download the pieces to the highlight video
        # and upload to whatever service we want.
        for message in kafka_consumer:
            payload_bytes = message.value

            # Decode and deserialize the payload
            payload = pickle_rick.loads(payload_bytes) or pickle_rick.dumps(bytes(dict()))
            print('Incoming event:', payload)

            timestamp = payload['timestamp']

            # Convert timestamp to a human readable format
            #timestamp = timestamp#- 14400 # Hacky fix for Twitch's timestamps
            readable_timestamp = str(datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'))

            # Build the highlight!
            highlight_title = f'Goal ({readable_timestamp})'

            # Get the video file and upload
            highlight_video_buffer = read_highlight(timestamp)
            print('Video size:', len(highlight_video_buffer), type(highlight_video_buffer))
            streamable_url = upload_highlight_streamable(highlight_title, highlight_video_buffer)
            
            del highlight_video_buffer

            print('Streamable link:', streamable_url)
    except KeyboardInterrupt:
        kafka_consumer.close()
        print('interrupted')
