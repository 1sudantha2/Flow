package io.github.aedev.flow.ui.screens.home

import android.content.Context
import android.util.Log
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import dagger.hilt.android.lifecycle.HiltViewModel
import dagger.hilt.android.qualifiers.ApplicationContext
import io.github.aedev.flow.R
import io.github.aedev.flow.data.local.HomeFeedCacheFilters
import io.github.aedev.flow.data.local.HomeFeedCacheRepository
import io.github.aedev.flow.data.local.ViewHistory
import io.github.aedev.flow.data.model.Video
import io.github.aedev.flow.data.model.toVideo
import io.github.aedev.flow.data.repository.YouTubeRepository
import io.github.aedev.flow.data.shorts.ShortsRepository
import io.github.aedev.flow.ui.components.FeedInvalidationBus
import io.github.aedev.flow.utils.PerformanceDispatcher
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.currentCoroutineContext
import javax.inject.Inject

/**
 * The home feed.
 *
 * The feed is YouTube's own `FEwhat_to_watch` recommendation feed, fetched with the signed-in
 * account's session when one exists — so what the user sees is what YouTube recommends to their
 * account (subscription uploads mixed with recommendations), paged through server continuation
 * tokens. There is no local ranking or profiling: ordering is the server's.
 *
 * Scroll-ahead prefetch ([HomePrefetchQueue]) and the last-feed disk cache stay: they are what
 * makes the feed feel instant and seamless, and they are pure plumbing, not analysis.
 */
@HiltViewModel
class HomeViewModel
    @Inject
    constructor(
        private val repository: YouTubeRepository,
        private val shortsRepository: ShortsRepository,
        private val playerPreferences: io.github.aedev.flow.data.local.PlayerPreferences,
        private val shortsQueueHandoff: io.github.aedev.flow.data.shorts.queue.ShortsQueueHandoff,
        private val persistentHomeFeedCache: HomeFeedCacheRepository,
        private val viewHistory: ViewHistory,
        @ApplicationContext private val appContext: Context,
    ) : ViewModel() {
        fun shortsShelfSource(
            shelf: List<Video>,
            tapped: Video,
        ) = shortsQueueHandoff.sourceForShelf(shelf, tapped)

        companion object {
            private const val TAG = "HomeViewModel"
            private const val UI_STATE_SUBSCRIPTION_TIMEOUT_MS = 5_000L
            private const val SHORTS_SHELF_SIZE = 24
        }

        private val channelMetadataEnrichmentInFlight =
            java.util.concurrent.ConcurrentHashMap
                .newKeySet<String>()

        private val _uiState = MutableStateFlow(HomeUiState())
        val uiState: StateFlow<HomeUiState> =
            _uiState
                .map(HomeUiState::withUniqueLazyContent)
                .stateIn(
                    scope = viewModelScope,
                    started = SharingStarted.WhileSubscribed(UI_STATE_SUBSCRIPTION_TIMEOUT_MS),
                    initialValue = _uiState.value.withUniqueLazyContent(),
                )

        private var homeContinuation: String? = null
        private var isInitialized = false
        private val homePrefetchQueue = HomePrefetchQueue()
        private val homePrefetchWorkerLock = Any()
        private var homePrefetchJob: Job? = null

        private val watchedVideoIds = MutableStateFlow<Set<String>>(emptySet())

        init {
            if (HomeFeedCache.isFresh()) {
                _uiState.update {
                    it.copy(
                        videos = HomeFeedCache.videos,
                        shorts = HomeFeedCache.shorts,
                        isLoading = false,
                        isFlowFeed = true,
                        lastRefreshTime = HomeFeedCache.timestamp,
                    )
                }
            } else {
                hydratePersistentHomeFeed()
                loadFlowFeed(forceRefresh = true)
                loadHomeShorts()
            }
        }

        fun initialize(context: Context) {
            if (isInitialized) return
            isInitialized = true

            viewModelScope.launch(PerformanceDispatcher.diskIO) {
                combine(
                    viewHistory.getVideoHistoryFlow(),
                    playerPreferences.hideWatchedVideosFromHome,
                    playerPreferences.watchedThreshold,
                    playerPreferences.continueWatchingEnabled,
                ) { history, hideWatched, threshold, continueWatchingEnabled ->
                    filterHomeHistory(
                        history = history,
                        hideWatchedVideos = hideWatched,
                        watchedThreshold = threshold,
                        continueWatchingEnabled = continueWatchingEnabled,
                    )
                }.collect { result ->
                    watchedVideoIds.value = result.watchedVideoIds
                    _uiState.update { state ->
                        val videos = state.videos.filterWatched(result.watchedVideoIds)
                        val shorts = state.shorts.filterWatched(result.watchedVideoIds)
                        if (videos != state.videos || shorts != state.shorts) {
                            HomeFeedCache.update(videos, shorts)
                        }
                        state.copy(
                            videos = videos,
                            shorts = shorts,
                            continueWatchingVideos = result.continueWatchingVideos,
                        )
                    }
                }
            }

            viewModelScope.launch {
                FeedInvalidationBus.events.collect { event ->
                    when (event) {
                        is FeedInvalidationBus.Event.ChannelBlocked -> {
                            HomeFeedCache.filterOut(channelId = event.channelId)
                            HomeFeedCache.filterOut(videoId = event.videoId)
                            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                                persistentHomeFeedCache.deleteChannel(event.channelId)
                                persistentHomeFeedCache.deleteVideo(event.videoId)
                            }
                            _uiState.update { state ->
                                state.copy(
                                    videos =
                                        state.videos.filter {
                                            it.id != event.videoId && it.channelId != event.channelId
                                        },
                                    shorts =
                                        state.shorts.filter {
                                            it.id != event.videoId && it.channelId != event.channelId
                                        },
                                )
                            }
                        }

                        is FeedInvalidationBus.Event.NotInterested -> {
                            HomeFeedCache.filterOut(videoId = event.videoId)
                            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                                persistentHomeFeedCache.deleteVideo(event.videoId)
                            }
                            _uiState.update { state ->
                                state.copy(
                                    videos = state.videos.filter { it.id != event.videoId },
                                    shorts = state.shorts.filter { it.id != event.videoId },
                                )
                            }
                        }

                        is FeedInvalidationBus.Event.MarkedWatched -> {
                            HomeFeedCache.filterOut(videoId = event.videoId)
                            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                                persistentHomeFeedCache.deleteVideo(event.videoId)
                            }
                            _uiState.update { state ->
                                state.copy(
                                    videos = state.videos.filter { it.id != event.videoId },
                                    shorts = state.shorts.filter { it.id != event.videoId },
                                )
                            }
                        }
                    }
                }
            }

            viewModelScope.launch {
                playerPreferences.effectiveHomeShortsShelfEnabled.collect { enabled ->
                    if (!enabled) {
                        _uiState.update { it.copy(shorts = emptyList()) }
                    } else if (_uiState.value.shorts.isEmpty()) {
                        loadHomeShorts()
                    }
                }
            }
        }

        fun onHomeVisible() {
            val state = _uiState.value
            startHomePrefetch(
                homePrefetchQueue.onVisible(
                    currentVideoCount = state.videos.size,
                    feedReady = state.isReadyForPrefetch(),
                ),
            )
        }

        fun onHomeHidden() {
            homePrefetchQueue.onHidden()
            synchronized(homePrefetchWorkerLock) {
                homePrefetchJob?.cancel()
            }
            _uiState.update { it.copy(isLoadingMore = false) }
        }

        fun onHomeViewportChanged(lastVisibleVideoIndex: Int) {
            val state = _uiState.value
            if (!state.isReadyForPrefetch()) return
            startHomePrefetch(
                homePrefetchQueue.onViewportChanged(
                    currentVideoCount = state.videos.size,
                    lastVisibleVideoIndex = lastVisibleVideoIndex,
                ),
            )
        }

        private fun HomeUiState.isReadyForPrefetch(): Boolean = videos.isNotEmpty() && !isLoading && isFlowFeed && hasMorePages

        private fun startHomePrefetch(request: HomePrefetchRequest?) {
            request ?: return
            val worker =
                synchronized(homePrefetchWorkerLock) {
                    if (homePrefetchJob?.isCompleted == false) return
                    viewModelScope
                        .launch(
                            context = PerformanceDispatcher.networkIO,
                            start = CoroutineStart.LAZY,
                        ) {
                            drainHomePrefetchQueue(request.generation)
                        }.also { homePrefetchJob = it }
                }
            worker.start()
        }

        private suspend fun drainHomePrefetchQueue(generation: Int) {
            var pagesLoaded = 0
            var emptyPageAttempts = 0
            var allowRestart = true
            try {
                while (pagesLoaded < HOME_PREFETCH_MAX_PAGES_PER_RUN) {
                    val state = _uiState.value
                    val request = homePrefetchQueue.currentRequest(state.videos.size) ?: break
                    if (request.generation != generation || !state.hasMorePages) break

                    _uiState.update { it.copy(isLoadingMore = true) }
                    if (loadNextPrefetchPage(generation)) {
                        pagesLoaded++
                        continue
                    }

                    // A page that appended nothing leaves the feed at the same length, so the
                    // viewport index cannot change and nothing would re-arm this queue. Retry a
                    // few times before giving up.
                    emptyPageAttempts++
                    if (emptyPageAttempts >= HOME_PREFETCH_EMPTY_PAGE_RETRIES) {
                        allowRestart = false
                        break
                    }
                    delay(HOME_PREFETCH_EMPTY_PAGE_BACKOFF_MS * emptyPageAttempts)
                }
                if (pagesLoaded >= HOME_PREFETCH_MAX_PAGES_PER_RUN) {
                    allowRestart = false
                }
            } catch (cancellation: CancellationException) {
                allowRestart = false
                throw cancellation
            } finally {
                val workerJob = currentCoroutineContext()[Job]
                val ownsLoadingState =
                    synchronized(homePrefetchWorkerLock) {
                        if (homePrefetchJob === workerJob) {
                            homePrefetchJob = null
                            true
                        } else {
                            false
                        }
                    }
                if (ownsLoadingState) {
                    _uiState.update { it.copy(isLoadingMore = false) }
                    if (allowRestart) {
                        startHomePrefetch(homePrefetchQueue.currentRequest(_uiState.value.videos.size))
                    }
                }
            }
        }

        private fun resetHomePrefetch() {
            homePrefetchQueue.reset()
            synchronized(homePrefetchWorkerLock) {
                homePrefetchJob?.cancel()
            }
            _uiState.update { it.copy(isLoadingMore = false) }
        }

        fun removeContinueWatchingEntry(videoId: String) {
            viewModelScope.launch {
                viewHistory.clearVideoHistory(videoId)
            }
        }

        private fun loadHomeShorts() {
            viewModelScope.launch {
                if (!playerPreferences.effectiveHomeShortsShelfEnabled.first()) return@launch
                try {
                    val shorts = shortsRepository.getHomeFeedShorts().map { it.toVideo() }
                    if (shorts.isNotEmpty()) {
                        _uiState.update {
                            it.copy(shorts = shorts.filterWatched(watchedVideoIds.value))
                        }
                    }
                } catch (e: Exception) {
                    Log.d(TAG, "home shorts shelf failed: ${e.message}")
                }
            }
        }

        private fun hydratePersistentHomeFeed() {
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                val cached =
                    runCatching {
                        persistentHomeFeedCache.loadLastFeed(cacheFilters())
                    }.getOrElse { emptyList() }
                if (cached.isEmpty()) return@launch
                val hydratedCached = repository.enrichLikelyCollabAvatarStacks(cached, limit = 8)

                _uiState.update { state ->
                    if (state.videos.isNotEmpty()) return@update state
                    val videos = hydratedCached.filterWatched(watchedVideoIds.value)
                    HomeFeedCache.update(videos, state.shorts)
                    state.copy(
                        videos = videos,
                        isFlowFeed = true,
                        error = null,
                        lastRefreshTime = System.currentTimeMillis(),
                    )
                }
                enrichVisibleChannelMetadata(hydratedCached)?.let {
                    persistentHomeFeedCache.saveLastFeed(it)
                }
            }
        }

        private suspend fun cacheFilters(): HomeFeedCacheFilters =
            HomeFeedCacheFilters(
                watchedVideoIds = watchedVideoIds.value,
                suppressedVideoIds = emptySet(),
                blockedChannelIds = emptySet(),
                suppressedChannelIds = emptySet(),
            )

        private fun loadFlowFeed(forceRefresh: Boolean = false) {
            if (_uiState.value.isLoading && !forceRefresh) return

            _uiState.update { it.copy(isLoading = true, error = null) }

            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                try {
                    val fetchStart = System.currentTimeMillis()
                    val page = repository.homeFeed().getOrElse { e ->
                        Log.w(TAG, "home feed failed: ${e.message}")
                        _uiState.update {
                            it.copy(
                                isLoading = false,
                                isRefreshing = false,
                                error = appContext.getString(R.string.error_failed_to_load_feed),
                            )
                        }
                        settleWithoutFeed()
                        return@launch
                    }
                    homeContinuation = page.continuation

                    val (shortsFromFeed, regularVideos) = page.videos.partition { it.isShort }
                    val now = System.currentTimeMillis()

                    if (shortsFromFeed.isNotEmpty() &&
                        playerPreferences.effectiveHomeShortsShelfEnabled.first()
                    ) {
                        _uiState.update { state ->
                            state.copy(
                                shorts =
                                    (state.shorts + shortsFromFeed.take(SHORTS_SHELF_SIZE))
                                        .distinctBy { it.id }
                                        .filterWatched(watchedVideoIds.value),
                            )
                        }
                    }

                    val videos = regularVideos.filterWatched(watchedVideoIds.value)
                    if (videos.isEmpty()) {
                        settleWithoutFeed()
                        return@launch
                    }

                    val spacedMix = repository.enrichLikelyCollabAvatarStacks(videos, limit = 8)
                    _uiState.update { state ->
                        state.copy(
                            videos = spacedMix,
                            isLoading = false,
                            isRefreshing = false,
                            hasMorePages = page.continuation != null,
                            isFlowFeed = true,
                            lastRefreshTime = now,
                        )
                    }
                    HomeFeedCache.update(spacedMix, _uiState.value.shorts)
                    persistentHomeFeedCache.saveLastFeed(spacedMix)
                    enrichVisibleChannelMetadata(spacedMix)?.let {
                        persistentHomeFeedCache.saveLastFeed(it)
                    }
                    loadHomeShorts()
                    Log.d(TAG, "home feed loaded ${spacedMix.size} videos in ${System.currentTimeMillis() - fetchStart}ms")
                } catch (e: Exception) {
                    _uiState.update {
                        it.copy(
                            isLoading = false,
                            isRefreshing = false,
                            error = appContext.getString(R.string.error_failed_to_load_feed),
                        )
                    }
                    settleWithoutFeed()
                }
            }
        }

        private suspend fun loadNextPrefetchPage(generation: Int): Boolean {
            val continuation = homeContinuation ?: return false
            return try {
                val page = repository.homeFeed(continuation).getOrNull() ?: return false
                if (!homePrefetchQueue.isCurrent(generation)) return false
                homeContinuation = page.continuation

                val (shortsFromFeed, regularVideos) = page.videos.partition { it.isShort }
                if (shortsFromFeed.isNotEmpty() &&
                    playerPreferences.effectiveHomeShortsShelfEnabled.first()
                ) {
                    _uiState.update { state ->
                        state.copy(
                            shorts =
                                (state.shorts + shortsFromFeed.take(SHORTS_SHELF_SIZE))
                                    .distinctBy { it.id }
                                    .filterWatched(watchedVideoIds.value),
                        )
                    }
                }

                var updatedSnapshot: List<Video>? = null
                var appendedPage = emptyList<Video>()
                _uiState.update { state ->
                    val existingVideoIds = state.videos.mapTo(HashSet()) { it.id }
                    appendedPage =
                        regularVideos
                            .filterWatched(watchedVideoIds.value)
                            .filterNot { it.id in existingVideoIds }
                    if (appendedPage.isEmpty()) {
                        if (page.continuation == null) {
                            return@update state.copy(hasMorePages = false)
                        }
                        return@update state
                    }
                    val tailChannels = state.videos.takeLast(2).map { it.channelId }
                    val updated = state.videos + spaceByChannel(appendedPage, seedRecent = tailChannels)
                    updatedSnapshot = updated
                    HomeFeedCache.update(updated, state.shorts)
                    state.copy(
                        videos = updated,
                        hasMorePages = page.continuation != null,
                    )
                }
                if (appendedPage.isEmpty()) return true
                val enriched = enrichVisibleChannelMetadata(appendedPage) ?: updatedSnapshot
                enriched?.let { persistentHomeFeedCache.saveLastFeed(it) }
                true
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (error: Exception) {
                Log.d(TAG, "home prefetch page failed: ${error.message}")
                false
            }
        }

        private suspend fun enrichVisibleChannelMetadata(videos: List<Video>): List<Video>? {
            val enriched = repository.enrichMissingChannelMetadata(videos)
            if (enriched == videos) return null

            val originalById = videos.associateBy { it.id }
            val updates =
                enriched
                    .filter { enrichedVideo -> originalById[enrichedVideo.id] != enrichedVideo }
                    .associateBy { it.id }
            var updatedSnapshot: List<Video>? = null
            _uiState.update { state ->
                val updated =
                    state.videos.map { current ->
                        updates[current.id]?.let(current::withChannelMetadataFrom) ?: current
                    }
                if (updated == state.videos) return@update state
                updatedSnapshot = updated
                HomeFeedCache.update(updated, state.shorts)
                state.copy(videos = updated)
            }
            return updatedSnapshot
        }

        fun enrichChannelMetadataIfMissing(video: Video) {
            val videoId = video.id
            val needsMetadata =
                video.channelId.isBlank() ||
                    !video.channelId.startsWith("UC") ||
                    video.channelThumbnailUrl.isBlank()
            if (!needsMetadata || !channelMetadataEnrichmentInFlight.add(videoId)) return

            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                try {
                    val enriched =
                        repository
                            .enrichMissingChannelMetadata(listOf(video), limit = 1)
                            .firstOrNull()
                            ?: return@launch
                    if (enriched == video) return@launch

                    _uiState.update { state ->
                        val updated =
                            state.videos.map { current ->
                                if (current.id != videoId) {
                                    current
                                } else {
                                    current.withChannelMetadataFrom(enriched)
                                }
                            }
                        if (updated == state.videos) {
                            state
                        } else {
                            HomeFeedCache.update(updated, state.shorts)
                            state.copy(videos = updated)
                        }
                    }
                } finally {
                    channelMetadataEnrichmentInFlight.remove(videoId)
                }
            }
        }

        /**
         * Nothing loaded and nothing cached: settle empty and offer a refresh rather than filling
         * the feed with unrelated content.
         */
        private fun settleWithoutFeed() {
            _uiState.update {
                it.copy(
                    isLoading = false,
                    isRefreshing = false,
                    hasMorePages = false,
                    isFlowFeed = false,
                )
            }
        }

        fun refreshFeed() {
            resetHomePrefetch()
            HomeFeedCache.clear()
            homeContinuation = null
            _uiState.update { it.copy(isRefreshing = true) }
            loadFlowFeed(forceRefresh = true)
        }

        fun retry() {
            resetHomePrefetch()
            loadFlowFeed(forceRefresh = true)
        }
    }
