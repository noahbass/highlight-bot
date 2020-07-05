import java.util.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import kotlin.ByteArray
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration

val KAFKA_SERVER = "127.0.0.1:9092"
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
fun get_corrected_timezone(original_timestamp: Int, original_timezone: String): Int {
    // TODO: Do this properly with pytz
    return original_timestamp + 14400
}

fun setup(topicName: String): Unit {
    // Check if the topic already exists in Kafka

    // Create topics if they don't exist

    println("Creating topic $topicName...")

    println("Topic $topicName already exists")

    return
}

fun createConsumer(brokerAddress: String): KafkaConsumer<String, String> {
    val consumerProperties = Properties()
    consumerProperties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = brokerAddress
    consumerProperties[ConsumerConfig.GROUP_ID_CONFIG] = "video-clip-service-consumer"
    consumerProperties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    consumerProperties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    consumerProperties[ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG] = "true"
    return KafkaConsumer<String, String>(consumerProperties)
}


fun serializeContextPayload(payload: ContextPayload): ByteArray {
    return payload.toJSON()
}

fun deserializeGoalPayload(byteArray: ByteArray): GoalPayload? {
    return try {
        val jsonString = byteArray.toString(charset("utf-8"))
        val gson = Gson()
        val json: GoalPayload = gson.fromJson(jsonString, GoalPayload::class.java)
        json
    } catch (e: Throwable) {
        null
    }
}

fun main(args: Array<String>) {
    println("Starting video clip worker service")

//    KAFKA_TOPICS.forEach { it -> setup(it) }
//    setup(KAFKA_OUTGOING_TOPIC)
//
//    val foo = byteArrayOf(123, 34, 110, 97, 109, 101, 34, 58, 32, 34, 100, 105, 110, 101, 115, 104, 34, 44, 32, 34, 99, 111, 100, 101, 34, 58, 32, 34, 100, 114, 45, 48, 49, 34, 125)

//    println(foo.toString(charset("utf-8")))
    val examplePayloadString = "{ \"timestamp\": 1593914523 }"
    val gson = Gson()
    val json: GoalPayload = gson.fromJson(examplePayloadString, GoalPayload::class.java)

    println(json.timestamp)

    val foo = ContextPayload(goalEventTimestamp = 439043, context = listOf("foo"))
    println("foobar:")
    println(foo.toJSON())

    var goalEvents = emptySet<Int>()

    val consumer = createConsumer(KAFKA_SERVER)

    consumer.subscribe(listOf(KAFKA_TOPIC_GOAL_EVENTS))

    while (true) {
        val records = consumer.poll(Duration.ofSeconds(1))

        records.forEach { it ->
            println("New record: ${it.key()}, ${it.value()}")
        }
    }

//
//    println(KAFKA_TOPICS)
}

