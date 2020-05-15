# highlight-bot

> highlight bot is a platform that uses AI to automagically create highlight videos from live FIFA 20 gameplay when goals are scored.

## Demo

highlight bot works by watching a live Twitch stream of FIFA gamelay. When it detects a goal, the 20 seconds before the goal are clipped as a highlight video. The highlight video is then uploaded to Streamable (highlight bot is extensible, so other hooks can be easily added). All of this happens in memory, no writes to disk (other than [Apache Kafka](https://kafka.apache.org/documentation/#introduction)'s internals).

[Demo video]

## Architecture

highlight bot is designed as a set of independent microservices, each with a unique role - from detecting goals with OCR to creating highlight videos. The microservices are arranged as a streaming data pipeline connected via [Apache Kafka](https://kafka.apache.org/documentation/#introduction) topics. Kafka is a real-time message broker that persists messages in an ordered queue so consumers can reliably access ordered data from a streaming data pipeline (important for this application).

highlight bot is somewhat designed to scale. Theoretically, with some minor code modifications in messaging metadata, highlight bot could watch multiple Twitch streams at once.

[Design here]

As mentioned before, highlight bot is extensible, so more hooks (built as services) can be added by the user. Each hook uses a different [Kafka consumer group identifier](https://kafka.apache.org/documentation/#intro_consumers) that subscribes to the x topic (thereby emulating a [fan-out/broadcast](https://en.wikipedia.org/wiki/Fan-out_(software))).

### The Microservices: Explained

highlight bot's 5 microservices are simple, independent services that perform just one task. Each service is either a producer, consumer, or both producer and consumer. Producers push data to a Kafka topic (stream). Consumers subscribe to a Kafka topic (or many Kafka topics) and data.

- stream-capture-service: TODO
- stream-metadata-service: TODO
- stream-timestamp-service: TODO
- video-clip-service: TODO
- highlight-worker: TODO

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

## TODO

- [ ] Clean up the code for the Python Kafka producer-consumer pattern (perhaps make a shared module for the pattern)
- [ ] Clean up some WET Python code
- [ ] Prettify event sending from the stream-capture-service
- [ ] Fix timestamp manipulation (with `pytz`) in the highlight worker
- [ ] Formalize the stream capture services and Kafka topics
- [ ] Reduce the size of the Docker images
- [ ] Create a user friendly web interface for configuring and monitoring highlight bot
- [ ] Add scalability for detecting goals from multiple streams at once

## License

[MIT](LICENSE)
