import os
import time
import streamlink
from PIL import Image
import cv2
import game_screen
import goal_event_producer


CHANNEL_NAME = os.getenv('CHANNEL_NAME')
KAFKA_TOPIC_GOAL_EVENTS = 'hbot.core.goal-events'


def frame_handler(frame, frame_count, current_state):
    cv2_im = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    image = Image.fromarray(cv2_im)
    frame_count += frame_rate

    # image.show() # debug
    new_state = game_screen.process_image(image)
    print('new_state:', new_state)

    if current_state is None:
        if None not in new_state.values():
            # current_state = new_state
            return new_state, frame_count

    # If any value is none in new_state, skip over
    if None not in new_state.values():
        # No values are none, check for goal
        goal_event_timestamp = new_state['timestamp']

        if new_state['home_score'] > current_state['home_score']:
            print('Home goal detected')
            # Send event to kafka
            goal_event_producer.send_goal_event(goal_event_timestamp, KAFKA_TOPIC_GOAL_EVENTS)
        elif new_state['away_score'] > current_state['away_score']:
            print('Away goal detected')
            goal_event_producer.send_goal_event(goal_event_timestamp, KAFKA_TOPIC_GOAL_EVENTS)
        
        # current_state = new_state
        return new_state, frame_count
    
    return current_state, frame_count


if __name__ == '__main__':
    # Twitch setup
    streams = streamlink.streams(f'https://twitch.tv/{CHANNEL_NAME}')
    
    try:
        stream = streams["720p"] # default (works well enough for OCR)
    except:
        stream = streams["720p60"] # backup

    fd = stream.open()
    stream_url = fd.writer.stream.url
    fd.close()

    capture = cv2.VideoCapture(stream_url)
    print('stream url:', stream_url)

    ok, frame = capture.read()
    frame_count = 0
    frame_rate = 120 # how many frames to skip over for each frame captured

    # Hacky Kafka setup
    goal_event_producer.setup(KAFKA_TOPIC_GOAL_EVENTS)

    # Hacky
    # TODO: Formalize this
    current_state = None

    # Debug:
    # time.sleep(14)
    # print('Away goal detected')
    # goal_event_producer.send_goal_event(int(time.time()), KAFKA_TOPIC_GOAL_EVENTS)

    while capture.isOpened():
        try:
            # Capture every 
            capture.set(cv2.CAP_PROP_POS_FRAMES, frame_count) # skip to next frame

            ok, frame = capture.read()
            position = capture.get(0)
            print('Current position:', position)

            if ok == True:
                current_state, frame_count = frame_handler(frame, frame_count, current_state)
            else:
                break
        except KeyboardInterrupt:
            break
    
    cv2.destroyAllWindows()
    capture.release()
