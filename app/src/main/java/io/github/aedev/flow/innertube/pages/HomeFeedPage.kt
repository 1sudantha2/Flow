package io.github.aedev.flow.innertube.pages

import io.github.aedev.flow.data.model.Video
import io.github.aedev.flow.innertube.pages.renderer.FeedItem
import io.github.aedev.flow.innertube.pages.renderer.FeedItemOwner
import io.github.aedev.flow.innertube.pages.renderer.toFeedItem
import io.github.aedev.flow.innertube.pages.renderer.toFeedItems
import kotlinx.serialization.json.JsonElement

/**
 * One page of the main-site home feed (`FEwhat_to_watch`) — YouTube's own recommendation feed,
 * personalized for the signed-in account when a session exists.
 */
data class HomeFeedPage(
    val videos: List<Video>,
    val continuation: String?,
)

/**
 * Parse a `FEwhat_to_watch` browse response.
 *
 * The initial page carries `contents.richGridRenderer.contents`, a continuation answers with
 * `onResponseReceivedActions[0].appendContinuationItemsAction.continuationItems` (or the older
 * `continuationContents.richGridContinuation`). Each list holds `richItemRenderer` items, shelf
 * sections (`richSectionRenderer`, flattened into the feed — the Shorts shelf items are shorts
 * and flow into the home shelf through [Video.isShort]) and a trailing `continuationItemRenderer`.
 */
internal fun JsonElement.toHomeFeedPage(): HomeFeedPage {
    val owner = FeedItemOwner()
    val videos = mutableListOf<Video>()
    var continuation: String? = null

    val itemLists =
        listOfNotNull(
            objectOrNull()?.get("contents")?.get("richGridRenderer"),
            objectOrNull()
                ?.get("onResponseReceivedActions")
                ?.arrayOrNull()
                ?.firstOrNull()
                ?.objectOrNull()
                ?.get("appendContinuationItemsAction")
                ?.objectOrNull()
                ?.get("continuationItems"),
            objectOrNull()?.get("continuationContents")?.get("richGridContinuation"),
        )

    itemLists.forEach { grid ->
        grid
            ?.objectOrNull()
            ?.get("contents")
            ?.arrayOrNull()
            .orEmpty()
            .forEach { entry ->
                val node = entry.objectOrNull() ?: return@forEach
                node["richItemRenderer"]
                    ?.objectOrNull()
                    ?.get("content")
                    ?.toFeedItem(owner)
                    .videoOrNull()
                    ?.let { videos.add(it) }
                // Shelf sections (e.g. "Shorts", "Watch it again") — their items are videos and
                // shorts like any other; flatten them in document order.
                node["richSectionRenderer"]
                    ?.objectOrNull()
                    ?.get("content")
                    ?.objectOrNull()
                    ?.get("shelfRenderer")
                    ?.let { shelf ->
                        videos += shelf["items"].toFeedItems(owner).mapNotNull { it.videoOrNull() }
                        continuation =
                            continuation
                                ?: shelf["items"]
                                    ?.arrayOrNull()
                                    .orEmpty()
                                    .firstNotNullOfOrNull { item ->
                                        item.objectOrNull()
                                            ?.get("continuationItemRenderer")
                                            ?.objectOrNull()
                                            ?.get("continuationEndpoint")
                                            ?.objectOrNull()
                                            ?.get("continuationCommand")
                                            ?.objectOrNull()
                                            ?.get("token")
                                            ?.stringOrNull()
                                    }
                    }
                continuation =
                    continuation
                        ?: node["continuationItemRenderer"]
                            ?.objectOrNull()
                            ?.get("continuationEndpoint")
                            ?.objectOrNull()
                            ?.get("continuationCommand")
                            ?.objectOrNull()
                            ?.get("token")
                            ?.stringOrNull()
            }
    }

    return HomeFeedPage(
        videos = videos.distinctBy { it.id },
        continuation = continuation,
    )
}

private fun FeedItem?.videoOrNull(): Video? =
    when (this) {
        is FeedItem.VideoItem -> video
        is FeedItem.ShortItem -> video
        else -> null
    }
