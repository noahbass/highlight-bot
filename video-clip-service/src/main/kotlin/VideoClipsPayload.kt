import com.google.gson.Gson

/**
 * An outgoing data class that describes the goal as a list of urls for the highlight video
 * and the timestamp of the goal event.
 */
data class VideoClipsPayload(
    val metadata: List<VideoClip>,
    val head: Timestamp,
    val tail: Timestamp
) {
    fun toJSON(): ByteArray {
        val gson = Gson()
        val json = gson.toJson(this)
        return json.toByteArray(charset("utf-8"))
    }
}
