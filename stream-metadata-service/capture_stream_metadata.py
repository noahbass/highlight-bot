import os
import time
import pickle
import ciso8601
import streamlink
import requests
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.client_async import KafkaClient
from kafka import KafkaProducer


KAFKA_SERVER = 'kafka:9092'
KAFKA_TOPIC = 'hbot.core.stream-metadata'
STREAM_QUALITY = 'best' # Quality for the highlight video
CHANNEL_NAME = os.getenv('CHANNEL_NAME')


# Take a string that is a datetime, return the corresponding unix timestamp (in seconds)
def parse_timestamp(timestamp_string):
    ts = ciso8601.parse_datetime(timestamp_string)
    return int(time.mktime(ts.timetuple()))


# Given a response of m3u8 text from Twitch, return a list of .ts
# video objects and metadata
def process_m3u8(m3u8_text):
    # Split by newline characters
    lines = m3u8_text.splitlines()
    lines = list(filter(lambda x: x.startswith('https') or\
                                  x.startswith('#EXT-X-PROGRAM-DATE-TIME') or\
                                  x.startswith('#EXT-X-MEDIA-SEQUENCE'), lines))

    if lines is None or lines == []:
        # Bad data
        return None, None

    # grab the sequence number (the first entry in the list), then remove the sequence
    sequence = int(lines[0].split(':')[1])
    lines = lines[1:]

    # Convert the timestamp to a unix timestamp (in seconds)
    # Note: Unix timestamps are in UTC
    lines = list(map(lambda x: parse_timestamp(':'.join(x.split(':')[1:])) if x.startswith('#') else x, lines))

    # Group lines together 
    it = iter(lines)
    video_data = list(zip(it, it))

    return sequence, video_data


def get_stream_url(channel):
    streams = streamlink.streams(f'https://twitch.tv/{channel}')

    try:
        stream = streams[STREAM_QUALITY] # default
    except:
        stream = streams["720p"] # backup

    # Get the stream url
    fd = stream.open()
    stream_url = fd.writer.stream.url
    fd.close()

    return stream_url


def work(stream_url: str) -> dict:
    m3u8_response = requests.get(stream_url)
    m3u8_text = m3u8_response.text
    
    now = int(time.time()) # unix timestamp

    payload = dict(timestamp=now)

    # Process the m3u8 data
    sequence_number, processed_stream_data = process_m3u8(m3u8_text)
    
    if sequence_number is None or processed_stream_data is None:
        # Handle bad data
        return None

    payload['sequence'] = sequence_number
    payload['data'] = processed_stream_data
    # print(processed_stream_data)
    return payload


if __name__ == '__main__':
    # First, check if the topic already exists in kafka
    kafka_client = KafkaClient(bootstrap_servers=KAFKA_SERVER,
                               api_version=(2, 5, 0))

    version = kafka_client.check_version()
    future = kafka_client.cluster.request_update()
    kafka_client.poll(future=future)

    metadata = kafka_client.cluster
    current_topics = metadata.topics()

    kafka_client.close()
    
    print('Active topics:', current_topics)

    if KAFKA_TOPIC not in current_topics:
        print(f'Creating topic {KAFKA_TOPIC}...')
        kafka_admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER,
                                                api_version=(2, 5, 0))
        
        topic_list = [NewTopic(name=KAFKA_TOPIC,
                                num_partitions=1,
                                replication_factor=1)]
        kafka_admin_client.create_topics(new_topics=topic_list, validate_only=False)

        kafka_admin_client.close()
    else:
        print(f'Topic {KAFKA_TOPIC} exists')

    
    # Create producer for the kafka topic do get ready to publish
    kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                                   api_version=(2, 5, 0))

    stream_url = get_stream_url(CHANNEL_NAME)
    print("Stream url:", stream_url)

    # Create a work loop to refresh the m3u8 file every 10 seconds
    try:
        while True:
            payload = work(stream_url)
            print('Payload size:', len(payload['data']))

            if payload is not None:
                # Pickle, then publish async message with payload to the kafka topic
                payload_bytes = pickle.dumps(payload)
                # print('Payload:', payload)
                kafka_producer.send(KAFKA_TOPIC, payload_bytes)

                # Debugging information
                print(f'Parsed m3u8 at {payload["timestamp"]}:')
                for timestamp, video_url in payload['data']:
                    print('  ', timestamp, video_url[-12:]) # Print end of video url for debugging
            else:
                print('Bad payload')

            time.sleep(10)
    except KeyboardInterrupt:
        kafka_producer.close()
        print('interrupted')
