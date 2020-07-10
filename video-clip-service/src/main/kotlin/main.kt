import java.util.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import kotlin.ByteArray
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.security.Provider
import java.sql.Time
import java.time.Duration

val KAFKA_SERVER = "kafka:9092"
val KAFKA_VERSION =  "2.5.0"
val KAFKA_TOPIC_VIDEO_CLIPS = "hbot.core.video-clips"
val KAFKA_TOPIC_GOAL_EVENTS = "hbot.core.goal-events"
val KAFKA_TOPICS = listOf(KAFKA_TOPIC_VIDEO_CLIPS, KAFKA_TOPIC_GOAL_EVENTS)
val KAFKA_OUTGOING_TOPIC = "hbot.core.highlight-worker"

// Correct the timezone for a timestamp.
// For some reason, Twitch likes to use unix timestamps that are already in UTC,
// therefore, move forward the goal event timestamps to match.
//
// Input: the original unix timestamp and the original timezone location.
//
// Output: Unix timestamp with the UTC location
fun getCorrectedTimezone(originalTimestamp: Timestamp, originalTimezone: String): Timestamp {
    // TODO: Do this properly
    return originalTimestamp + 14400
}

fun setup(topicName: String): Unit {
    // TODO
    // Check if the topic already exists in Kafka

    // Create topics if they don't exist

    println("Creating topic $topicName...")

    println("Topic $topicName already exists")

    return
}

fun createConsumer(brokerAddress: String): KafkaConsumer<String, ByteArray> {
    val consumerProperties = Properties()
    consumerProperties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = brokerAddress
    consumerProperties[ConsumerConfig.GROUP_ID_CONFIG] = "stream_video_timestamp_range"
    consumerProperties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    consumerProperties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
    consumerProperties[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = "true"
    return KafkaConsumer<String, ByteArray>(consumerProperties)
}

fun createProducer(brokerAddress: String): KafkaProducer<String, ByteArray> {
    val properties = Properties()
    properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = brokerAddress
    properties["key.serializer"] = StringSerializer::class.java
    properties["value.serializer"] = ByteArraySerializer::class.java
    properties[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = "true"
    return KafkaProducer<String, ByteArray>(properties)
}


// Send a payload to a kafka topic using the given consumer instance
@ExperimentalStdlibApi
fun sendHighlightPayload(kafkaProducer: KafkaProducer<String, ByteArray>, topicName: String, payload: Highlight): Unit {
    // Serialize to JSON, then transform into byte array
    // TODO: replace with Protobufs
    val gson = Gson()
    val jsonString = gson.toJson(payload)
    val jsonByteArray = jsonString.encodeToByteArray()

    // Prepare and send
    val record = ProducerRecord<String, ByteArray>(topicName, jsonByteArray)
    kafkaProducer.send(record)
}


fun serializeContextPayload(payload: VideoClipsPayload): ByteArray {
    return payload.toJSON()
}

fun deserializeContextPayload(jsonString: String): VideoClipsPayload? {
    return try {
        val gson = Gson()
        val json: VideoClipsPayload = gson.fromJson(jsonString, VideoClipsPayload::class.java)
        json
    } catch (e: Throwable) {
        null
    }
}

fun deserializeGoalPayload(jsonString: String): GoalEventPayload? {
    return try {
        val gson = Gson()
        val json: GoalEventPayload = gson.fromJson(jsonString, GoalEventPayload::class.java)
        json
    } catch (e: Throwable) {
        null
    }
}

fun deserializePayload(byteArray: ByteArray): Any? {
    try {
        val jsonString = byteArray.toString(charset("utf-8"))

//        println("Deserializing $jsonString")

        // Try payload as Goal Payload
        val goalPayloadObject = deserializeGoalPayload(jsonString)

        // payload is Goal Payload
        goalPayloadObject?.let { return it }

        // Try payload as Context Payload
        val contextPayloadObject = deserializeContextPayload(jsonString)

        // paylaod is Context Payload
        contextPayloadObject?.let { return it }

        // No match for payload
        return null
    } catch (e: Throwable) {
        null
    }


    // Couldn't deserialize incoming payload
    return null
}

// Check if the goal event timestamp is within the current video context.
// If so, save the current video metadata context and other details needed
// to create the highlight clip.
// If not, save the timestamp in `goal_events` for later.
fun checkGoalContext(goalEventTimestamp: Timestamp, currentState: ServiceState): Boolean {
    println("Context: ${currentState.currentHead}, ${currentState.currentTail}, ${goalEventTimestamp}")

    if (currentState.currentVideoClipMetadataContext == emptySet<VideoClip>()) {
        // No metadata context available, so nothing to clip
        return false
    }

    if (currentState.currentHead == null || currentState.currentTail == null) {
        // No head or tail context is available, so nothing to clip
        return false
    }

    if (currentState.currentTail <= goalEventTimestamp && goalEventTimestamp <= currentState.currentHead) {
        // Found context for the goal event!
        return true
    }

    // Context for the goal event not found within the current context
    return false
}

data class Highlight(
    val goalEventTimestamp: Timestamp,
    val videoClip: List<VideoClip>
)


fun handleVideoClipPayload(payload: VideoClipsPayload, currentState: ServiceState): Pair<List<Highlight>, ServiceState> {
    // Get the new metadata
    val metadata = payload.metadata
    val newHead = payload.head
    val newTail = payload.tail

    // Check goalEvents for any matches to the updated video clips
    // Note that there could be multiple matches
    val matchedGoalEvents = currentState.goalEvents.toMutableSet().filter { checkGoalContext(it, currentState) }

    // For any match, remove that event from goalEvents
    val updatedGoalEvents = currentState.goalEvents - matchedGoalEvents

    val newState = ServiceState(updatedGoalEvents, newHead, newTail, metadata)

    println("Video clips message:  head: ${newState.currentHead}, tail: ${newState.currentTail}, ${newState.currentTail!! - newState.currentHead!!} seconds")


    if (matchedGoalEvents.isEmpty()) {
        // There were no matches, so return empty video clip list with new state
        println("No matches")
        println("-----")
        return Pair(emptyList(), newState)
    }

    // There were matches, so return matched clips with the new state
    // with the metadata
    val matchedHighlights = matchedGoalEvents.toList().map { Highlight(it, currentState.currentVideoClipMetadataContext) }

    println("Matches made for $matchedGoalEvents")
    println("-----")

    return Pair(matchedHighlights, newState)
}

fun handleGoalEventPayload(payload: GoalEventPayload, currentState: ServiceState): Pair<Highlight?, ServiceState> {
    // For some reason, Twitch timestamps and real-world timestamps don't match up (different timezones?)
    // TODO: fix the timestamp issue
    val goalEventTimestamp = payload.timestamp // getCorrectedTimezone(payload.timestamp, "America/New_York")

    if (checkGoalContext(goalEventTimestamp, currentState)) {
        // Found a match for the goal event (no state change needed)
        println("   Have context: goal scored at timestamp $goalEventTimestamp")

        return Pair(Highlight(goalEventTimestamp, currentState.currentVideoClipMetadataContext), currentState)
    }

    // No match found for the goal event, so add the goal event timestamp to the current state
    // and updated the the state.
    val goalEvents = currentState.goalEvents.union(setOf(goalEventTimestamp))
    val newState = ServiceState(goalEvents, currentState.currentHead, currentState.currentTail, currentState.currentVideoClipMetadataContext)

    println("   No context yet: goal scored at timestamp $goalEventTimestamp")

    return Pair(null, newState)
}


@ExperimentalStdlibApi
fun main(args: Array<String>) {
    val producer = createProducer(KAFKA_SERVER)
    val consumer = createConsumer(KAFKA_SERVER)
    consumer.subscribe(listOf(KAFKA_TOPIC_GOAL_EVENTS, KAFKA_TOPIC_VIDEO_CLIPS))

    // Timestamps from incoming Goal Events that have not been matched yet.
    // When a match is made, the timestamp is removed from the set.
    // TODO: Cache this in redis (b/c this is super important)
    var state = ServiceState(emptySet(), null, null, emptyList())

    // Work:
    // Wait for video clip payloads and goal event payloads.
    // When a new goal event comes in, match it up with the corresponding video metadata.
    // If the goal event timestamp is within the video metadata timestamps, then there is a match
    // and an event can be published to the outgoing topic.
    while (true) {
        val records = consumer.poll(Duration.ofSeconds(1))

        records.forEach { record ->
            // Try to deserialize the record (it could be a Goal record or a Context record)
            val rawRecord: ByteArray = record.value()
            val jsonString = rawRecord.toString(charset("utf-8"))

            if (record.topic() == KAFKA_TOPIC_GOAL_EVENTS) {
                val payload = deserializeGoalPayload(jsonString)

                payload?.let {
                    val (highlight, newState) = handleGoalEventPayload(payload = payload, currentState = state)
                    state = newState

                    highlight?.let {
                        // There is a highlight to send outgoing
                        println("Highlight sent to $KAFKA_OUTGOING_TOPIC")
                        sendHighlightPayload(producer, KAFKA_OUTGOING_TOPIC, highlight)
                    }
                }
            } else if (record.topic() == KAFKA_TOPIC_VIDEO_CLIPS) {
                val payload = deserializeContextPayload(jsonString)

                payload?.let {
//                    println("New context payload record: $it")

                    val (highlights, newState) = handleVideoClipPayload(payload = payload, currentState = state)
                    state = newState

                    highlights.forEach { highlight ->
                        println("Highlight sent to $KAFKA_OUTGOING_TOPIC")
                        sendHighlightPayload(producer, KAFKA_OUTGOING_TOPIC, highlight)
                    }
                }
            }
        }
    }
}


//    KAFKA_TOPICS.forEach { it -> setup(it) }
//    setup(KAFKA_OUTGOING_TOPIC)
//
//    val foo = byteArrayOf(123, 34, 110, 97, 109, 101, 34, 58, 32, 34, 100, 105, 110, 101, 115, 104, 34, 44, 32, 34, 99, 111, 100, 101, 34, 58, 32, 34, 100, 114, 45, 48, 49, 34, 125)

//    println(foo.toString(charset("utf-8")))
// val examplePayloadString = "{ \"timestamp\": 1593914523 }"
// val gson = Gson()
// val json: GoalPayload = gson.fromJson(examplePayloadString, GoalPayload::class.java)

// println(json.timestamp)

// val foo = ContextPayload(goalEventTimestamp = 439043, context = listOf("foo"))
// println("foobar:")
// println(foo.toJSON())