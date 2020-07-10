data class ServiceState(
    val goalEvents: Set<Timestamp>, // Timestamps from incoming Goal Events that have not been matched yet.
                                    // When a match is made, the timestamp is removed from the set.
                                    // TODO: Cache this in redis (b/c this is super important)
    val currentHead: Timestamp?, // Head and tail of the currently available video clips urls in redis
    val currentTail: Timestamp?,
    val currentVideoClipMetadataContext: List<VideoClip> // The current Video Clip Context
)
