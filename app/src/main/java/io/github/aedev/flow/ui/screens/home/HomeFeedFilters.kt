package io.github.aedev.flow.ui.screens.home

import io.github.aedev.flow.data.model.Video

internal fun List<Video>.filterWatched(watchedIds: Set<String>): List<Video> {
    if (watchedIds.isEmpty() || isEmpty()) return this
    return filter { it.id !in watchedIds }
}
