import datetime
import time
import pickle as pickle_rick
import requests
import pytz
import redis
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.client_async import KafkaClient
from kafka import KafkaConsumer, KafkaProducer


KAFKA_SERVER = '127.0.0.1:9092'
KAFKA_TOPIC_VIDEO_CLIPS = 'hbot.worker.video-clips'
KAFKA_TOPIC_GOAL_EVENTS = 'hbot.worker.goal-events'
KAFKA_TOPICS = [KAFKA_TOPIC_VIDEO_CLIPS, KAFKA_TOPIC_GOAL_EVENTS]
KAFKA_OUTGOING_TOPIC = 'hbot.worker.highlight-worker'


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


def get_goal_context(current_video_clip_metadata_context, current_head, current_tail, goal_event_timestamp):
    if current_video_clip_metadata_context is None:
        # No metadata context available, so nothing to clip
        return None

    if current_head is None or current_tail is None:
        # No head or tail context is available, so nothing to clip
        return None

    if current_tail <= goal_event_timestamp and goal_event_timestamp <= current_head:
        # Found context for the goal event!
        return current_video_clip_metadata_context

    # Context for the goal event not found within the current context
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


# Create the context payload pickled
def get_context_payload(context: list, goal_event_timestamp: int):
    payload = dict(context=context, goal_event_timestamp=goal_event_timestamp)
    payload_bytes = pickle_rick.dumps(payload)
    return payload_bytes


if __name__ == '__main__':
    print('Starting video worker consumer')
    
    for topic in KAFKA_TOPICS:
        setup(topic)
    setup(KAFKA_OUTGOING_TOPIC)

    r = redis.Redis(host='127.0.0.1', port=6379, db=0)

    kafka_consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER,
                                   api_version=(2, 5, 0),
                                   group_id='stream_video_timestamp_range',
                                   auto_offset_reset='earliest',
                                   enable_auto_commit=True)
    kafka_consumer.subscribe(topics=tuple(KAFKA_TOPICS))

    # Create producer for the kafka topic do get ready to publish
    kafka_producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092',
                                   api_version=(2, 5, 0))

    try:
        # Timestamps from incoming Goal Events that have not been matched yet.
        # When a match is made, the timestamp is removed from the set.
        # TODO: Cache this in redis (b/c this is super important)
        goal_events = set()

        # Head and tail of the currently available video clips urls in redis
        current_head, current_tail = None, None

        # The current Video Clip Context
        current_video_clip_metadata_context = None

        # Wait for Video Clip Messages and Goal Event messages.
        # When a new Goal Event comes in, match it up with the corresponding
        # video metadata.
        # If the Goal Event timestamp is within the video metadata timestamps,
        # then there is a match up.
        for message in kafka_consumer:
            message_topic = message.topic
            payload_bytes = message.value

            payload = pickle_rick.loads(payload_bytes) or pickle_rick.dumps(bytes(dict()))

            if message_topic == KAFKA_TOPIC_VIDEO_CLIPS:
                # Clip metadata (timestamps and video urls)
                metadata = payload['metadata']
                current_video_clip_metadata_context = metadata

                # New head (first timestamp) and new tail (last timestamp)
                new_head, new_tail = payload['head'], payload['tail']
                current_head, current_tail = new_head, new_tail

                # Save the new head and tail
                print('Video Clips message:', 'head:', new_head, ' tail:', new_tail, f'({new_head-new_tail} seconds)')
                print('-----')

                # Check the goal_events set for any matches
                # Copy to avoid changing the set during iteration
                for goal_event_timestamp in goal_events.copy():
                    metadata_context = get_goal_context(current_video_clip_metadata_context,
                                                        current_head,
                                                        current_tail,
                                                        goal_event_timestamp)
                    
                    if metadata_context is not None:
                        # Found a match. TODO: send to kafka highlight worker topic
                        goal_events.remove(goal_event_timestamp)
                        print(f'  Have context: goal scored at timestamp {goal_event_timestamp}')
                        payload = get_context_payload(metadata_context, goal_event_timestamp)
                        kafka_producer.send(KAFKA_OUTGOING_TOPIC, payload)
            if message_topic == KAFKA_TOPIC_GOAL_EVENTS:
                print('Goal Event message:')
                # Take the match set of video clips as the context for the goal event,
                # then process the video.
                goal_event_timestamp = payload['timestamp']

                # Convert timestamp to correct format
                # TODO: Do this properly
                user_timezone = 'America/New_York'
                goal_event_timestamp = get_corrected_timezone(goal_event_timestamp, user_timezone)

                # Check if the goal event timestamp is within the current video context.
                # If so, save the current video metadata context and other details needed
                # to create the highlight clip.
                # If not, save the timestamp in `goal_events` for later.
                metadata_context = get_goal_context(current_video_clip_metadata_context,
                                                    current_head,
                                                    current_tail,
                                                    goal_event_timestamp)

                if metadata_context is None:
                    # Add goal event to the `goal_events` set for later
                    goal_events.add(goal_event_timestamp)
                    print(f'  No context yet: goal scored at timestamp {goal_event_timestamp}')
                else:
                    # TODO: send to kafka highlight worker topic
                    print(f'  Have context: goal scored at timestamp {goal_event_timestamp}')
                    payload = get_context_payload(metadata_context, goal_event_timestamp)
                    kafka_producer.send(KAFKA_OUTGOING_TOPIC, payload)
                
                print('-----')

    except KeyboardInterrupt:
        kafka_consumer.close()
        print('interrupted')
