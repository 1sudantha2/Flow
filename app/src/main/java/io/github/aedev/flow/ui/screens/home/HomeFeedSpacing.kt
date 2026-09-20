package io.github.aedev.flow.ui.screens.home

import io.github.aedev.flow.data.model.Video

/**
 * Greedy reorder that keeps same-channel items at least `gap` slots apart when possible; order is
 * otherwise preserved. seedRecent primes the cooldown with the prior page's tail to space appends.
 */
internal fun spaceByChannel(
    videos: List<Video>,
    gap: Int = 1,
    seedRecent: List<String> = emptyList(),
): List<Video> {
    if (videos.size < 2) return videos
    val remaining = videos.toMutableList()
    val out = ArrayList<Video>(videos.size)
    val recent = ArrayDeque<String>()
    seedRecent.takeLast(gap).forEach { recent.addLast(it) }
    while (remaining.isNotEmpty()) {
        val idx =
            remaining
                .indexOfFirst { it.channelId.isBlank() || it.channelId !in recent }
                .let { if (it < 0) 0 else it }
        val pick = remaining.removeAt(idx)
        out.add(pick)
        if (pick.channelId.isNotBlank()) {
            recent.addLast(pick.channelId)
            while (recent.size > gap) recent.removeFirst()
        }
    }
    return out
}

internal fun Video.withChannelMetadataFrom(enriched: Video): Video {
    val avatarUrl = enriched.channelThumbnailUrl.ifBlank { channelThumbnailUrl }
    return copy(
        channelId = enriched.channelId.ifBlank { channelId },
        channelName = enriched.channelName.ifBlank { channelName },
        channelThumbnailUrl = avatarUrl,
        channelThumbnailUrls =
            if (avatarUrl.isNotBlank()) {
                (
                    listOf(avatarUrl) +
                        enriched.channelThumbnailUrls +
                        channelThumbnailUrls
                ).distinct()
            } else {
                channelThumbnailUrls
            },
    )
}
