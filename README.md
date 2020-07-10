# highlight-bot

> highlight bot is a platform that uses AI to automagically create highlight videos from live FIFA 20 gameplay when goals are scored. It's written in Python and Kotlin with Kafka as the real-time message broker.

**Contents**

- Demo
- Architecture
    - The Microservices: Explained
    - The Hooks: Explained
- Installation
- Usage
- Additional Theoretical Features
- TODO
- License

## Demo

highlight bot works by watching a live Twitch stream of FIFA gamelay. When it detects a goal, the 20 seconds before the goal are clipped as a highlight video. The highlight video is then uploaded to Streamable (highlight bot is extensible, so other hooks can be easily added). All of this is stateless and everything happens in memory, no writes to disk (other than [Apache Kafka](https://kafka.apache.org/documentation/#introduction)'s internals) - however, a write-to-disk hook could be added by the user (more on that below).

[Demo video - TODO]

## Architecture

<p align="center">
  <img src="./architecture.svg" />
</p>

highlight bot is designed as a set of independent microservices, each with a unique role - from detecting goals with OCR to creating highlight videos. The microservices are arranged as a streaming data pipeline connected via [Apache Kafka](https://kafka.apache.org/documentation/#introduction) topics. Kafka is a real-time message broker that persists messages in an ordered queue so consumers can reliably access ordered data from a streaming data pipeline (order is important for this application). *Sidenote: Kafka is a distributed streaming platform that is highly durable, scalable, and resilient. However, in this application, Kafka is used mostly its real-time data streaming with single partitions, and not so much for its scalability/distributed features.*

highlight bot is somewhat designed to scale. Theoretically, with some minor code modifications in messaging metadata for Kafka, highlight bot could watch multiple Twitch streams at once.

Because of the modular design, the stream capture service could be swapped out for a lookalike service to detect events in other video games (or other feature detection techniques in live video feeds).

As mentioned before, highlight bot is extensible, so more hooks (a type of service) can be added by the user. Each hook uses a different [Kafka consumer group identifier](https://kafka.apache.org/documentation/#intro_consumers) that subscribes to the x topic (thereby emulating a [fan-out/broadcast](https://en.wikipedia.org/wiki/Fan-out_(software))).

### The Microservices: Explained

highlight bot's 5 microservices are simple, independent services that perform just one task. Each service is either a producer, consumer, or both producer and consumer. Producers push data to a Kafka topic (stream). Consumers subscribe to a Kafka topic (or many Kafka topics) and data.

- stream-capture-service: Monitors the Twitch stream for events. It uses OCR to determine the score, time, and other information.
- stream-metadata-service: Collects stream metadata from the Twitch stream. The actual video from the stream isn't downloaded until the data reaches the highlight worker, therefore limiting the work the system needs to do to only essential tasks.
- stream-timestamp-service: Processes, cleans, and trims Twitch m3u8 stream data (gets it ready for the Video clip service).
- video-clip-service: Enriches Twitch stream data with stream events (such as a goal event). When an event happens and stream data is available, the video clip service sends the metadata to the highlight worker.
- highlight-worker: The highlight worker downloads the video data needed for the highlight clip, saves the clip to a local volume, then sends a fanout message to any hooks that may be listening.

### The Hooks: Explained

Hooks are a special type of service in the highlight bot platform.

TODO: More explanation

Hooks recieve messages in a fanout/broadcast from the highlight worker. To participate as a consumer in the broadcast, each hook identifies itself with a unique Kafka consumer group identifier. Due to this fanout design, an unlimited number of hooks can be added to highlight bot.

## Installation

highlight bot requires [Docker](https://www.docker.com/products/docker-desktop).

## Usage

To detecting goals and generating highlight clips:

```sh
$ nano docker-compose.yml # edit the CHANNEL_NAME environment variable in docker-compose.yml
$ docker-compose up # this could take a while on first run
# use control+c to stop
```

To cleanup after usage (removes leftover containers, volumes, and networks):

```sh
$ docker-compose down -v
```

## Additional Theoretical Features

As previously mentioned, highlight bot is extensible. Because of the modular design, any of the services can be swapped out or modified to support other features. Some of these are trivial, some are nontrivial:

TODO

## TODO

- [ ] Use Protobuf (or similar library) to create and maintain stable contracts between the microservices (aka API contracts)
- [ ] Clean up the code for the Python Kafka producer-consumer pattern (perhaps make a shared module for the pattern)
- [ ] Better timestamp sync between Stream Capture Service and Stream Metadata Service
- [ ] Scale the number of highlight workers (so multiple highlight videos can be processed in parallel)
- [ ] Better logging and platform monitoring (Kafka, the services, etc.)
- [ ] Clean up some WET Python code
- [ ] Prettify event sending from the stream-capture-service
- [ ] Fix timestamp manipulation (with `pytz`) in the highlight worker
- [ ] Formalize the stream capture services and Kafka topics
- [ ] Reduce the size of the Docker images
- [ ] Create a user friendly web interface for configuring and monitoring highlight bot
- [ ] Add scalability for detecting goals from multiple streams at once
- [ ] Anaylze CPU usage of the services (the stream capture service in particular)

## License

[MIT](LICENSE)
