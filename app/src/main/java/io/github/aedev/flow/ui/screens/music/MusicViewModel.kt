package io.github.aedev.flow.ui.screens.music

import android.content.Context
import android.util.Log
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import dagger.hilt.android.lifecycle.HiltViewModel
import dagger.hilt.android.qualifiers.ApplicationContext
import io.github.aedev.flow.R
import io.github.aedev.flow.data.local.PlayerPreferences
import io.github.aedev.flow.data.music.DownloadManager
import io.github.aedev.flow.data.music.model.primaryArtistKey
import io.github.aedev.flow.data.music.MusicCache
import io.github.aedev.flow.data.music.YouTubeMusicService
import io.github.aedev.flow.data.music.model.ArtistDetails
import io.github.aedev.flow.data.music.model.CommunityMusicPlaylist
import io.github.aedev.flow.data.music.model.DailyDiscoverItem
import io.github.aedev.flow.data.music.model.MusicItemType
import io.github.aedev.flow.data.music.model.MusicPlaylist
import io.github.aedev.flow.data.music.model.MusicTrack
import io.github.aedev.flow.data.music.model.PlaylistDetails
import io.github.aedev.flow.data.music.model.RelatedMusic
import io.github.aedev.flow.data.newmusic.InnertubeMusicService
import io.github.aedev.flow.data.music.MusicRecommendationAlgorithm
import io.github.aedev.flow.data.music.MusicSection
import io.github.aedev.flow.innertube.YouTube
import io.github.aedev.flow.innertube.models.BrowseEndpoint
import io.github.aedev.flow.innertube.pages.ArtistItemsPage
import io.github.aedev.flow.innertube.pages.HomePage
import io.github.aedev.flow.innertube.pages.MoodAndGenres
import io.github.aedev.flow.player.EnhancedMusicPlayerManager
import io.github.aedev.flow.utils.PerformanceDispatcher
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import javax.inject.Inject
import kotlin.random.Random

@HiltViewModel
class MusicViewModel
    @Inject
    constructor(
        @ApplicationContext private val context: Context,
        private val musicRecommendationAlgorithm: MusicRecommendationAlgorithm,
        private val subscriptionRepository: io.github.aedev.flow.data.local.SubscriptionRepository,
        private val playlistRepository: io.github.aedev.flow.data.music.PlaylistRepository,
        private val localPlaylistRepository: io.github.aedev.flow.data.local.PlaylistRepository,
        private val downloadManager: DownloadManager,
        private val playerPreferences: PlayerPreferences,
    ) : ViewModel() {
        companion object {
            private const val SESSION_CACHE_LIMIT = 48
            private const val SECONDARY_CONTENT_START_CAP_MS = 1_500L
            private const val COMMUNITY_ARTIST_SEEDS = 3
            private const val COMMUNITY_TRACK_SEEDS = 2
            private const val COMMUNITY_PLAYLIST_COUNT = 6
            private const val COMMUNITY_PREVIEW_TRACKS = 10
        }

        private val _uiState = MutableStateFlow(MusicUiState())

        // WhileSubscribed (not Eagerly) is load-bearing for battery: it makes
        // _uiState.subscriptionCount reflect real UI visibility, which gates the
        // per-track shelf recomposition below. Hidden artists are combined here
        // so feedback removes an artist from every shelf reactively.
        val uiState: StateFlow<MusicUiState> =
            _uiState
                .map { state -> state.withUniqueLazyContent() }
                .flowOn(PerformanceDispatcher.parsing)
                .stateIn(
                    scope = viewModelScope,
                    started = SharingStarted.WhileSubscribed(5_000),
                    initialValue = _uiState.value.withUniqueLazyContent(),
                )

        private fun isUiVisible(): Boolean = _uiState.subscriptionCount.value > 0

        private fun MusicTrack.isAudioMusicCandidate(): Boolean {
            val usableDuration = duration == 0 || duration in 30..1200
            return itemType == MusicItemType.SONG && !isVideoSong && videoId.isNotBlank() && usableDuration
        }

        private fun List<MusicTrack>.audioMusicOnly(): List<MusicTrack> = filter { it.isAudioMusicCandidate() }.distinctBy { it.videoId }

        init {
            loadMusicContent()

            viewModelScope.launch(PerformanceDispatcher.parsing) {
                downloadManager.downloadedTracks.collect { tracks ->
                    _uiState.update { state ->
                        state.copy(downloadedTrackIds = tracks.map { it.track.videoId }.toSet())
                    }
                }
            }

            // Track changes only recompose the shelves while the Music UI is on
            // screen; background playback (screen off, other tabs) marks them
            // stale instead — otherwise endless radio would trigger a full
            // network + ranking pass every few minutes all night.
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                var lastTrackId: String? = EnhancedMusicPlayerManager.currentTrack.value?.videoId
                EnhancedMusicPlayerManager.currentTrack.collectLatest { activeTrack ->
                    if (activeTrack != null && !activeTrack.videoId.isNullOrBlank()) {
                        if (activeTrack.videoId != lastTrackId) {
                            lastTrackId = activeTrack.videoId
                            if (isUiVisible()) {
                                refreshLocalShelves()
                            } else {
                                shelvesStale = true
                            }
                        }
                    }
                }
            }

            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                _uiState.subscriptionCount.collect { count ->
                    if (count > 0 && homeStale) {
                        homeStale = false
                        shelvesStale = false
                        refresh()
                    } else if (count > 0 && shelvesStale) {
                        shelvesStale = false
                        refreshLocalShelves()
                    }
                }
            }

            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                combine(playerPreferences.contentLanguage, playerPreferences.trendingRegion) { language, region -> language to region }
                    .distinctUntilChanged()
                    .drop(1)
                    .collect {
                        if (isUiVisible()) refresh() else homeStale = true
                    }
            }

            // Speed dial ranked by the comfort surface, so the tiles are the
            // truest "most yours" rather than raw shelf concatenation order.
            // Writing speedDialTracks re-emits _uiState, but the source triple is
            // unchanged then, so distinctUntilChanged breaks the loop.
            viewModelScope.launch(PerformanceDispatcher.parsing) {
                _uiState
                    .map { Triple(it.history, it.forYouTracks, it.listenAgain) }
                    .distinctUntilChanged()
                    .collectLatest { (history, forYou, listenAgain) ->
                        val pool = (history + forYou + listenAgain).audioMusicOnly().take(26)
                        if (pool.isNotEmpty()) {
                            _uiState.update { it.copy(speedDialTracks = pool) }
                        }
                    }
            }
        }

        @Volatile
        private var shelvesStale = false

        @Volatile
        private var homeStale = false

        private val artistDetailsCache = ConcurrentHashMap<String, Deferred<ArtistDetails?>>()
        private val relatedCache = ConcurrentHashMap<String, Deferred<RelatedMusic?>>()
        private val relatedFromNetwork = AtomicInteger()

        private suspend fun <V> ConcurrentHashMap<String, Deferred<V?>>.fetchOnce(
            key: String,
            fetch: suspend () -> V?,
        ): V? {
            if (size >= SESSION_CACHE_LIMIT) clear()
            val deferred =
                computeIfAbsent(key) {
                    viewModelScope.async(PerformanceDispatcher.networkIO, start = CoroutineStart.LAZY) {
                        runCatching { fetch() }.getOrNull()
                    }
                }
            val value = deferred.await()
            if (value == null) remove(key, deferred)
            return value
        }

        private suspend fun cachedRelated(
            seedId: String,
            seed: MusicTrack? = null,
        ): RelatedMusic? =
            relatedCache.fetchOnce(seedId) {
                InnertubeMusicService.getRelatedPage(seedId, audioOnly = true)
                    ?.also { relatedFromNetwork.incrementAndGet() }
            }

        private suspend fun cachedArtistDetails(channelId: String): ArtistDetails? =
            artistDetailsCache.fetchOnce(channelId) {
                InnertubeMusicService.fetchArtistDetails(channelId)
            }

        /**
         * The local shelves, derived purely from listening history: On Repeat leads with the
         * most recent rotation and Rediscover resurfaces the tail of the history. Zero network.
         */
        private suspend fun refreshLocalShelves() {
            try {
                val history = playlistRepository.history.firstOrNull().orEmpty().audioMusicOnly()
                val onRepeat = history.take(16)
                val onRepeatIds = onRepeat.mapTo(HashSet()) { it.videoId }
                val rediscover =
                    history
                        .drop(16)
                        .filterNot { it.videoId in onRepeatIds }
                        .take(12)
                _uiState.update {
                    it.copy(
                        onRepeatTracks = if (onRepeat.size >= 2) onRepeat else it.onRepeatTracks,
                        rediscoverTracks = if (rediscover.size >= 3) rediscover else emptyList(),
                    )
                }
            } catch (e: Exception) {
                Log.e("MusicViewModel", "Error loading local shelves", e)
            }
        }

        /**
         *  PERFORMANCE OPTIMIZED: Load all music content progressively
         *  Each section loads independently to show content as fast as possible
         */
        private fun loadMusicContent(force: Boolean = false) {
            viewModelScope.launch(PerformanceDispatcher.diskIO) {
                val cachedTrending = MusicCache.getTrendingMusic(100)
                val cachedResult =
                    try {
                        musicRecommendationAlgorithm.loadMusicHome()
                    } catch (e: Exception) {
                        emptyList<MusicSection>() to null
                    }

                val cachedSections = cachedResult.first

                if (cachedTrending != null || cachedSections.isNotEmpty()) {
                    withContext(PerformanceDispatcher.parsing) {
                        // Apply cached data immediately
                        if (cachedSections.isNotEmpty()) {
                            processHomeSections(cachedSections)
                            cachedResult.second?.let { continuation ->
                                _uiState.update { it.copy(homeContinuation = continuation) }
                            }
                        }

                        cachedTrending?.let { trend ->
                            _uiState.update {
                                it.copy(
                                    trendingSongs = trend,
                                    allSongs = if (it.selectedFilter == null) trend else it.allSongs,
                                )
                            }
                        }

                        _uiState.update { it.copy(isLoading = false) }
                    }
                } else {
                    _uiState.update { it.copy(isLoading = true, error = null) }
                }
            }

            // On Repeat / Rediscover — served entirely from local listening history, zero network.
            viewModelScope.launch(PerformanceDispatcher.diskIO) {
                refreshLocalShelves()
            }

            // 1. CRITICAL: Trending / Charts (Fastest & Most Important)
            val trendingJob =
                viewModelScope.launch(PerformanceDispatcher.networkIO) {
                    val trending =
                        withTimeoutOrNull(8_000L) {
                            try {
                                // Try to get charts first for high quality trending data
                                val charts = InnertubeMusicService.fetchCharts()
                                if (charts != null) {
                                    _uiState.update {
                                        it.copy(
                                            chartPlaylists = charts.playlists,
                                            chartArtists = charts.artists,
                                            chartCountryCode = charts.countryCode,
                                        )
                                    }
                                }
                                if (charts != null && charts.songs.isNotEmpty()) {
                                    MusicCache.cacheTrendingMusic(100, charts.songs)
                                    charts.songs
                                } else {
                                    val trending = YouTubeMusicService.fetchTrendingMusic(100)
                                    MusicCache.cacheTrendingMusic(100, trending)
                                    trending
                                }
                            } catch (e: Exception) {
                                Log.e("MusicViewModel", "Error loading trending/charts", e)
                                null
                            }
                        }

                    trending?.let { trend ->
                        _uiState.update {
                            it.copy(
                                trendingSongs = trend,
                                allSongs = if (it.selectedFilter == null) trend else it.allSongs,
                                isLoading = false,
                            )
                        }
                    }

                }

            // 2. IMPORTANT: Home Sections (Dynamic Content)
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                var skippedFreshCache = false
                val homeResult =
                    withTimeoutOrNull(10_000L) {
                        // Reduced timeout
                        try {
                            if (force) {
                                musicRecommendationAlgorithm.refreshMusicHome()
                            } else {
                                // Cache inside its 4 h TTL: the cached pass already rendered it.
                                musicRecommendationAlgorithm.refreshMusicHomeIfStale()
                                    ?: (emptyList<MusicSection>() to null).also { skippedFreshCache = true }
                            }
                        } catch (e: Exception) {
                            Log.e("MusicViewModel", "Error refreshing home sections", e)
                            emptyList<MusicSection>() to null
                        }
                    } ?: (emptyList<MusicSection>() to null)

                val homeSections = homeResult.first
                val homeContinuation = homeResult.second

                val homeChips = musicRecommendationAlgorithm.getHomeChips()
                _uiState.update { it.copy(homeChips = homeChips) }

                if (homeSections.isNotEmpty()) {
                    processHomeSections(homeSections)
                    _uiState.update { it.copy(homeContinuation = homeContinuation) }
                } else if (!skippedFreshCache &&
                    _uiState.value.forYouTracks.isEmpty() && _uiState.value.dynamicSections.isEmpty()
                ) {
                    val recs = musicRecommendationAlgorithm.getRecommendations(24).audioMusicOnly()
                    if (recs.isNotEmpty()) {
                        _uiState.update { it.copy(forYouTracks = recs) }
                    }
                }
                if (_uiState.value.trendingSongs.isNotEmpty() || homeSections.isNotEmpty()) {
                    _uiState.update { it.copy(isLoading = false) }
                }
            }

            // 3. SECONDARY: History (Disk IO)
            viewModelScope.launch(PerformanceDispatcher.diskIO) {
                val history =
                    withTimeoutOrNull(5_000L) {
                        try {
                            playlistRepository.history.firstOrNull() ?: emptyList()
                        } catch (e: Exception) {
                            emptyList()
                        }
                    } ?: emptyList()

                if (history.isNotEmpty()) {
                    _uiState.update {
                        it.copy(
                            history = history,
                            // Raw history is only an emergency placeholder — never over a composed shelf.
                            forYouTracks =
                                if (it.forYouTracks.isEmpty()) {
                                    history.audioMusicOnly().take(24)
                                } else {
                                    it.forYouTracks
                                },
                            isLoading = false,
                        )
                    }
                }
            }

            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                withTimeoutOrNull(SECONDARY_CONTENT_START_CAP_MS) { trendingJob.join() }
                loadSecondaryContent()
            }
        }

        private fun loadSecondaryContent() {
            // 4. CONTENT: New Releases (Albums & Tracks)
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                withTimeoutOrNull(10_000L) {
                    try {
                        // Fetch Album Releases (New Feature)
                        val albums = InnertubeMusicService.fetchNewReleases()
                        if (albums.isNotEmpty()) {
                            _uiState.update { it.copy(topAlbums = albums) }
                        }

                        val newReleases = YouTubeMusicService.fetchNewReleases(40)
                        if (newReleases.isNotEmpty()) {
                            _uiState.update { it.copy(newReleases = newReleases) }
                        }
                        Unit
                    } catch (e: Exception) {
                        Log.e("MusicViewModel", "Error loading new releases", e)
                    }
                }
            }

            // 5. CONTENT: Moods & Genres (New Section)
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                withTimeoutOrNull(8_000L) {
                    try {
                        val moods = InnertubeMusicService.fetchMoodAndGenres()
                        if (moods.isNotEmpty()) {
                            _uiState.update { it.copy(moodsAndGenres = moods) }
                        }
                        Unit
                    } catch (e: Exception) {
                        Log.e("MusicViewModel", "Error loading moods", e)
                    }
                }
            }

            // 6. CONTENT: Featured Playlists
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                withTimeoutOrNull(12_000L) {
                    try {
                        val history =
                            try {
                                playlistRepository.history.firstOrNull() ?: emptyList()
                            } catch (e: Exception) {
                                emptyList()
                            }

                        val query =
                            if (history.isNotEmpty()) {
                                val topArtists =
                                    history
                                        .groupBy { it.artist }
                                        .map { it.key to it.value.size }
                                        .sortedByDescending { it.second }
                                        .take(3)
                                        .map { it.first }
                                        .filter { !it.isNullOrBlank() }
                                        .shuffled()

                                val selectedArtist = topArtists.firstOrNull()
                                if (selectedArtist != null) {
                                    "$selectedArtist playlist"
                                } else {
                                    "curated music playlists 2026"
                                }
                            } else {
                                "curated music playlists 2026"
                            }

                        Log.d("MusicViewModel", "Personalized playlists query: $query")
                        val playlists = YouTubeMusicService.searchPlaylists(query, 10)
                        if (playlists.isNotEmpty()) {
                            _uiState.update { it.copy(featuredPlaylists = playlists) }
                        } else {
                            val fallback = YouTubeMusicService.searchPlaylists("curated music playlists 2026", 10)
                            if (fallback.isNotEmpty()) {
                                _uiState.update { it.copy(featuredPlaylists = fallback) }
                            }
                        }
                        Unit
                    } catch (e: Exception) {
                        Log.e("MusicViewModel", "Error loading playlists", e)
                    }
                }
            }

            // 7. BACKGROUND: Popular Artists & Genre Content
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                withTimeoutOrNull(12_000L) {
                    try {
                        val tracks = YouTubeMusicService.fetchPopularArtistMusic(50)
                        if (tracks.isNotEmpty()) {
                            MusicCache.cacheGenreTracks("Popular Artists", 50, tracks)
                            val currentGenreTracks = _uiState.value.genreTracks.toMutableMap()
                            currentGenreTracks["Popular Artists"] = tracks

                            val genres = YouTubeMusicService.getPopularGenres()
                            _uiState.update {
                                it.copy(
                                    genreTracks = currentGenreTracks,
                                    genres = listOf("Popular Artists") + genres,
                                )
                            }
                        }
                        Unit
                    } catch (e: Exception) {
                        Log.e("MusicViewModel", "Error loading popular artists", e)
                    }
                }

                // Load specific genres in background
                val genreList = listOf("Pop", "Rock", "Hip Hop", "R&B", "Electronic")
                val genreMap = mutableMapOf<String, List<MusicTrack>>()

                supervisorScope {
                    genreList
                        .map { genre ->
                            async(PerformanceDispatcher.networkIO) {
                                withTimeoutOrNull(8_000L) {
                                    try {
                                        val tracks = musicRecommendationAlgorithm.getGenreContent(genre)
                                        if (tracks.isNotEmpty()) {
                                            genre to tracks
                                        } else {
                                            null
                                        }
                                    } catch (e: Exception) {
                                        null
                                    }
                                }
                            }
                        }.forEach { deferred ->
                            deferred.await()?.let { (genre, tracks) ->
                                genreMap[genre] = tracks
                            }
                        }
                }

                if (genreMap.isNotEmpty()) {
                    _uiState.update {
                        val updated = it.genreTracks.toMutableMap()
                        updated.putAll(genreMap)
                        it.copy(genreTracks = updated)
                    }
                }
            }

            // 8. BACKGROUND: Explore Page
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                val explore = InnertubeMusicService.fetchExplore()
                if (explore != null) {
                    _uiState.update { it.copy(explorePage = explore) }
                }
            }

            // 9. DYNAMIC CONTENT: Similar To & Vibes
            loadDynamicContent()

            // 10. DAILY DISCOVER: seed-based carousel
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                loadDailyDiscover()
            }

            // 11. COMMUNITY: human-curated playlists based on listening history
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                loadCommunityPlaylists()
            }
        }

        private fun String.isCuratedPlaylistId(): Boolean = !startsWith("RD") && !startsWith("OLAK")

        private fun MusicPlaylist.isCommunityPlaylistCandidate(): Boolean {
            val normalizedAuthor = author.trim()
            return normalizedAuthor.isNotBlank() &&
                !normalizedAuthor.equals("YouTube", true) &&
                !normalizedAuthor.equals("YouTube Music", true) &&
                id.isCuratedPlaylistId()
        }

        private suspend fun loadCommunityPlaylists() {
            try {
                val history =
                    withContext(PerformanceDispatcher.diskIO) {
                        playlistRepository.history.firstOrNull() ?: emptyList()
                    }.audioMusicOnly()
                val trackSeeds = history.distinctBy { it.primaryArtistKey() }.take(COMMUNITY_TRACK_SEEDS)
                val artistKeys =
                    history
                        .mapNotNull { it.channelId.takeIf { key -> key.startsWith("UC") } }
                        .distinct()
                        .take(COMMUNITY_ARTIST_SEEDS)
                        .ifEmpty {
                            _uiState.value.trendingSongs
                                .map { it.channelId }
                                .filter { it.startsWith("UC") }
                                .distinct()
                                .take(COMMUNITY_ARTIST_SEEDS)
                        }
                if (trackSeeds.isEmpty() && artistKeys.isEmpty()) return

                val candidates =
                    supervisorScope {
                        val fromArtists =
                            artistKeys.map { key ->
                                async(PerformanceDispatcher.networkIO) {
                                    val details = cachedArtistDetails(key) ?: return@async emptyList()
                                    details.featuredOn.filterNot { it.author.equals(details.name, ignoreCase = true) }
                                }
                            }
                        val fromTracks =
                            trackSeeds.map { seed ->
                                async(PerformanceDispatcher.networkIO) {
                                    cachedRelated(seed.videoId, seed)?.playlists.orEmpty()
                                }
                            }
                        (fromArtists + fromTracks).awaitAll().flatten()
                    }.filter { it.isCommunityPlaylistCandidate() }
                        .distinctBy { it.id }
                        .shuffled()
                        .take(COMMUNITY_PLAYLIST_COUNT)
                if (candidates.isEmpty()) return

                val communityItems =
                    supervisorScope {
                        candidates
                            .map { playlist ->
                                async(PerformanceDispatcher.networkIO) {
                                    val allTracks =
                                        InnertubeMusicService
                                            .fetchPlaylistDetails(playlist.id)
                                            ?.tracks
                                            ?: return@async null
                                    val tracks = allTracks.audioMusicOnly().take(COMMUNITY_PREVIEW_TRACKS)
                                    if (tracks.isEmpty()) return@async null
                                    CommunityMusicPlaylist(
                                        playlist =
                                            playlist.copy(
                                                trackCount = allTracks.size,
                                                thumbnailUrl = playlist.thumbnailUrl.ifBlank { tracks.first().thumbnailUrl },
                                            ),
                                        tracks = tracks,
                                    )
                                }
                            }.awaitAll()
                            .filterNotNull()
                    }

                if (communityItems.isNotEmpty()) {
                    _uiState.update { it.copy(communityPlaylists = communityItems) }
                }
            } catch (e: Exception) {
                Log.e("MusicViewModel", "Error loading community playlists", e)
            }
        }

        private fun loadDynamicContent() {
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                val history = playlistRepository.history.firstOrNull() ?: emptyList()
                val blocks = mutableListOf<SimilarToBlock>()

                if (history.isNotEmpty()) {
                    try {
                        val topArtists =
                            history
                                .groupBy { it.artist }
                                .mapValues { it.value.size }
                                .toList()
                                .sortedByDescending { it.second }
                                .take(10)
                                .shuffled()
                                .take(2)

                        blocks +=
                            topArtists
                                .map { (artistName, _) ->
                                    async(PerformanceDispatcher.networkIO) {
                                        val artistTrack = history.find { it.artist == artistName }
                                        if (artistTrack == null || artistTrack.channelId.isBlank()) return@async null
                                        similarToBlock(artistTrack, title = artistName, seedId = artistTrack.channelId, isArtistSeed = true)
                                    }
                                }.awaitAll()
                                .filterNotNull()

                        val recentTrack = history.firstOrNull()
                        if (
                            recentTrack != null &&
                            recentTrack.videoId.isNotBlank() &&
                            blocks.none { it.similar.title == recentTrack.title || it.similar.title == recentTrack.artist }
                        ) {
                            similarToBlock(recentTrack, title = recentTrack.title, seedId = recentTrack.videoId, isArtistSeed = false)
                                ?.let { blocks += it }
                        }
                    } catch (e: Exception) {
                        Log.e("MusicViewModel", "Error loading similar to sections", e)
                    }
                }
                val similarSections = blocks.mapTo(mutableListOf()) { it.similar }

                // B. Random Vibe Playlists
                val vibes = listOf("Focus", "Relaxing", "Energize", "Commute", "Party", "Romance", "Sad", "Sleep", "Workout")
                val vibe = vibes.random()

                try {
                    val playlists = YouTubeMusicService.searchPlaylists("$vibe music playlists", 10)
                    if (playlists.isNotEmpty()) {
                        val playlistTracks =
                            playlists.map { playlist ->
                                MusicTrack(
                                    videoId = playlist.id,
                                    title = playlist.title,
                                    artist = playlist.author,
                                    thumbnailUrl = playlist.thumbnailUrl,
                                    duration = 0,
                                    itemType = MusicItemType.PLAYLIST,
                                )
                            }
                        similarSections.add(
                            MusicSection(
                                title = context.getString(R.string.section_vibe_vibes, vibe),
                                subtitle = context.getString(R.string.subtitle_community_playlists),
                                tracks = playlistTracks,
                            ),
                        )
                    }
                } catch (e: Exception) {
                    Log.e("MusicViewModel", "Error loading vibe playlists", e)
                }

                if (similarSections.isNotEmpty()) {
                    _uiState.update {
                        it.copy(
                            similarToSections = similarSections,
                            otherPerformanceSections = blocks.mapNotNull { block -> block.otherPerformances },
                            moreFromArtistSections =
                                blocks
                                    .mapNotNull { block ->
                                        block.moreFromArtist
                                    }.distinctBy { section -> section.seedId },
                        )
                    }
                }
            }
        }

        private suspend fun similarToBlock(
            seed: MusicTrack,
            title: String,
            seedId: String,
            isArtistSeed: Boolean,
        ): SimilarToBlock? {
            val related = cachedRelated(seed.videoId, seed) ?: return null
            val songs = related.tracks.audioMusicOnly()
            if (songs.isEmpty()) return null
            val random = Random(_uiState.value.sessionSeed xor seedId.hashCode().toLong())
            val artistId = related.seedArtistId ?: seed.artists.firstOrNull()?.id ?: seed.channelId.takeIf { it.startsWith("UC") }
            val artistName = seed.artists.firstOrNull { it.id == artistId }?.name ?: seed.artist
            val performances = related.otherPerformances.filter { it.videoId != seed.videoId }.distinctBy { it.videoId }
            return SimilarToBlock(
                similar =
                    MusicSection(
                        title = title,
                        label = context.getString(R.string.similar_to),
                        thumbnailUrl = seed.thumbnailUrl,
                        seedId = seedId,
                        isArtistSeed = isArtistSeed,
                        tracks = SimilarToSections.mixed(songs, related.similarArtists, related.playlists, random),
                    ),
                otherPerformances =
                    performances.takeIf { it.isNotEmpty() }?.let {
                        MusicSection(
                            title = related.seed?.title ?: seed.title,
                            label = context.getString(R.string.section_other_performances),
                            seedId = seedId,
                            tracks = it,
                        )
                    },
                moreFromArtist =
                    if (artistId != null && related.artistAlbums.isNotEmpty()) {
                        MusicSection(
                            title = artistName,
                            label = context.getString(R.string.section_more_from),
                            seedId = artistId,
                            isArtistSeed = true,
                            tracks = related.artistAlbums.map { it.asCollectionTrack(MusicItemType.ALBUM) },
                        )
                    } else {
                        null
                    },
            )
        }

        fun loadMorePlaylistTracks() {
            val currentPlaylist = _uiState.value.selectedPlaylist ?: _uiState.value.playlistDetails ?: return
            val continuation = currentPlaylist.continuation ?: return
            if (_uiState.value.isMoreLoading) return

            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                _uiState.update { it.copy(isMoreLoading = true) }
                try {
                    val (newTracks, nextContinuation) = YouTubeMusicService.fetchPlaylistContinuation(currentPlaylist.id, continuation)

                    _uiState.update { state ->
                        val updatedPlaylist =
                            currentPlaylist.copy(
                                tracks = currentPlaylist.tracks + newTracks,
                                continuation = nextContinuation,
                                trackCount = currentPlaylist.trackCount + newTracks.size,
                            )
                        state.copy(
                            selectedPlaylist = updatedPlaylist,
                            playlistDetails = updatedPlaylist,
                            isMoreLoading = false,
                        )
                    }
                } catch (e: Exception) {
                    _uiState.update { it.copy(isMoreLoading = false) }
                }
            }
        }

        fun loadArtistItems(
            browseId: String,
            params: String?,
        ) {
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                _uiState.update { it.copy(isArtistItemsLoading = true, artistItemsPage = null) }
                YouTube
                    .artistItems(BrowseEndpoint(browseId, params))
                    .onSuccess { page ->
                        _uiState.update { it.copy(artistItemsPage = page, isArtistItemsLoading = false) }
                    }.onFailure {
                        _uiState.update { it.copy(isArtistItemsLoading = false) }
                    }
            }
        }

        fun loadMoreArtistItems() {
            val continuation = _uiState.value.artistItemsPage?.continuation ?: return
            if (_uiState.value.isMoreLoading) return

            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                _uiState.update { it.copy(isMoreLoading = true) }
                YouTube
                    .artistItemsContinuation(continuation)
                    .onSuccess { page ->
                        _uiState.update {
                            it.copy(
                                isMoreLoading = false,
                                artistItemsPage =
                                    it.artistItemsPage?.copy(
                                        items = it.artistItemsPage.items + page.items,
                                        continuation = page.continuation,
                                    ),
                            )
                        }
                    }.onFailure {
                        _uiState.update { it.copy(isMoreLoading = false) }
                    }
            }
        }

        // Helper to process sections to avoid code duplication
        private suspend fun processHomeSections(sections: List<MusicSection>) {
            val quickPicks =
                sections
                    .find {
                        it.title.contains("Quick picks", true) ||
                            it.title.contains("Start radio", true) ||
                            it.title.contains("Recommended", true) ||
                            it.title.contains("Mixed for you", true)
                    }?.tracks
                    ?.audioMusicOnly()
                    .orEmpty()

            val recommended =
                sections
                    .find {
                        it.title.contains("Mixed for you", true) ||
                            it.title.contains("Recommended", true) ||
                            it.title.contains("Listen again", true)
                    }?.tracks
                    ?.audioMusicOnly()
                    .orEmpty()

            val musicVideosForYou =
                sections
                    .find {
                        it.title.contains("Music videos for you", true)
                    }?.tracks ?: emptyList()

            val musicVideos =
                sections
                    .find {
                        it.title.contains("Music videos", true) || it.title.contains("Videos", true)
                    }?.tracks ?: musicVideosForYou

            val livePerformances =
                sections
                    .find {
                        it.title.contains("Live performances", true) ||
                            (it.title.contains("Live", true) && it.title.contains("performance", true))
                    }?.tracks ?: emptyList()

            val longListens =
                sections
                    .find {
                        it.title.contains("Long listens", true)
                    }?.tracks ?: emptyList()

            val listenAgain =
                sections
                    .find {
                        it.title.contains("Listen again", true)
                    }?.tracks
                    ?.audioMusicOnly() ?: emptyList()

            _uiState.update { currentState ->
                currentState.copy(
                    forYouTracks = quickPicks.ifEmpty { currentState.forYouTracks },
                    recommendedTracks = recommended.ifEmpty { currentState.recommendedTracks },
                    listenAgain = listenAgain,
                    musicVideos = musicVideos,
                    musicVideosForYou = musicVideosForYou,
                    livePerformances = livePerformances,
                    longListens = longListens,
                    dynamicSections = sections,
                )
            }
        }

        fun setHomeChip(chip: HomePage.Chip?) {
            _uiState.update { it.copy(selectedHomeChip = chip) }
            if (chip != null && chip.endpoint != null) {
                viewModelScope.launch(PerformanceDispatcher.networkIO) {
                    _uiState.update { it.copy(isLoading = true) }
                    try {
                        val response = YouTube.home(params = chip.endpoint.params).getOrNull()
                        response?.let { home ->
                            processHomeSections(musicRecommendationAlgorithm.parseHomeSections(home))
                        }
                    } catch (e: Exception) {
                        Log.e("MusicViewModel", "Error filtering by chip", e)
                    } finally {
                        _uiState.update { it.copy(isLoading = false) }
                    }
                }
            } else {
                loadMusicContent()
            }
        }

        fun retry() {
            loadMusicContent()
        }

        fun refresh() {
            relatedCache.clear()
            artistDetailsCache.clear()
            _uiState.update { it.copy(isLoading = true) }
            loadMusicContent(force = true)
        }

        /**
         *  PERFORMANCE OPTIMIZED: Fetch artist details with timeout
         */
        fun fetchArtistDetails(channelId: String) {
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                _uiState.value =
                    _uiState.value.copy(
                        isArtistLoading = true,
                        artistDetails = null,
                    )

                supervisorScope {
                    val detailsDeferred =
                        async(PerformanceDispatcher.networkIO) {
                            withTimeoutOrNull(10_000L) {
                                YouTubeMusicService.fetchArtistDetails(channelId)
                            }
                        }

                    val subscriptionDeferred =
                        async(PerformanceDispatcher.diskIO) {
                            subscriptionRepository.isSubscribed(channelId).firstOrNull() ?: false
                        }

                    val details = detailsDeferred.await()
                    val isSubscribed = subscriptionDeferred.await()

                    _uiState.value =
                        _uiState.value.copy(
                            isArtistLoading = false,
                            artistDetails = details?.copy(isSubscribed = isSubscribed),
                        )
                }
            }
        }

        fun toggleFollowArtist(artist: ArtistDetails) {
            viewModelScope.launch(PerformanceDispatcher.diskIO) {
                if (artist.isSubscribed) {
                    subscriptionRepository.unsubscribe(artist.channelId)
                } else {
                    subscriptionRepository.subscribe(
                        io.github.aedev.flow.data.local.ChannelSubscription(
                            channelId = artist.channelId,
                            channelName = artist.name,
                            channelThumbnail = artist.thumbnailUrl,
                            isMusic = true,
                        ),
                    )
                }

                // Update UI state
                val currentDetails = _uiState.value.artistDetails
                if (currentDetails?.channelId == artist.channelId) {
                    _uiState.value =
                        _uiState.value.copy(
                            artistDetails = currentDetails.copy(isSubscribed = !artist.isSubscribed),
                        )
                }
            }
        }

        fun clearArtistDetails() {
            _uiState.value = _uiState.value.copy(artistDetails = null)
        }

        /**
         *  PERFORMANCE OPTIMIZED: Fetch playlist details with timeout
         */
        fun fetchPlaylistDetails(playlistId: String) {
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                _uiState.value = _uiState.value.copy(isPlaylistLoading = true, playlistDetails = null)

                // Try local first (fast path)
                val localPlaylist =
                    withContext(PerformanceDispatcher.diskIO) {
                        localPlaylistRepository.getPlaylistInfo(playlistId)
                    }

                if (localPlaylist != null) {
                    val videos =
                        withContext(PerformanceDispatcher.diskIO) {
                            localPlaylistRepository.getPlaylistVideosFlow(playlistId).firstOrNull() ?: emptyList()
                        }
                    val tracks =
                        videos.map { video ->
                            MusicTrack(
                                videoId = video.id,
                                title = video.title,
                                artist = video.channelName,
                                thumbnailUrl = video.thumbnailUrl,
                                duration = (video.duration / 1000).toInt(),
                                sourceUrl = "", // Not needed for local playback usually
                            )
                        }

                    val details =
                        PlaylistDetails(
                            id = localPlaylist.id,
                            title = localPlaylist.name,
                            thumbnailUrl = localPlaylist.thumbnailUrl,
                            author = context.getString(R.string.you),
                            trackCount = tracks.size,
                            description = localPlaylist.description,
                            tracks = tracks,
                        )

                    _uiState.value =
                        _uiState.value.copy(
                            isPlaylistLoading = false,
                            playlistDetails = details,
                            selectedPlaylist = details,
                        )
                    return@launch
                }

                // Fallback to remote with timeout
                try {
                    val details =
                        withTimeoutOrNull(12_000L) {
                            YouTubeMusicService.fetchPlaylistDetails(playlistId)
                        }
                    _uiState.value =
                        _uiState.value.copy(
                            isPlaylistLoading = false,
                            playlistDetails = details,
                            selectedPlaylist = details,
                        )
                } catch (e: Exception) {
                    _uiState.value =
                        _uiState.value.copy(
                            isPlaylistLoading = false,
                            error = context.getString(R.string.error_failed_to_load_playlist),
                        )
                }
            }
        }

        fun loadCommunityPlaylist(genre: String) {
            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                _uiState.value = _uiState.value.copy(isPlaylistLoading = true, playlistDetails = null)
                try {
                    var tracks = _uiState.value.genreTracks[genre]

                    if (tracks == null || tracks.isEmpty()) {
                        // Fetch if not in state (e.g. new ViewModel instance)
                        tracks = withTimeoutOrNull(10_000L) {
                            YouTubeMusicService.fetchMusicByGenre(genre, 30)
                        } ?: emptyList()
                    }

                    val playlistDetails =
                        PlaylistDetails(
                            id = "community_$genre",
                            title = genre,
                            thumbnailUrl = tracks.firstOrNull()?.thumbnailUrl ?: "",
                            author = context.getString(R.string.playlist_author_community),
                            trackCount = tracks.size,
                            description = context.getString(R.string.playlist_description_community, genre),
                            tracks = tracks,
                        )
                    _uiState.value =
                        _uiState.value.copy(
                            isPlaylistLoading = false,
                            playlistDetails = playlistDetails,
                        )
                } catch (e: Exception) {
                    Log.e("MusicViewModel", "Error loading community playlist", e)
                    _uiState.value = _uiState.value.copy(isPlaylistLoading = false)
                }
            }
        }

        fun clearPlaylistDetails() {
            _uiState.value = _uiState.value.copy(playlistDetails = null)
        }

        fun loadMoreHomeContent() {
            val currentContinuation = _uiState.value.homeContinuation ?: return
            if (_uiState.value.isMoreLoading) return

            viewModelScope.launch(PerformanceDispatcher.networkIO) {
                _uiState.update { it.copy(isMoreLoading = true) }

                try {
                    val result = musicRecommendationAlgorithm.loadHomeContinuation(currentContinuation)
                    val newSections = result.first
                    val nextContinuation = result.second

                    if (newSections.isNotEmpty()) {
                        val currentSections = _uiState.value.dynamicSections.toMutableList()
                        currentSections.addAll(newSections)
                        _uiState.update {
                            it.copy(
                                dynamicSections = currentSections,
                                homeContinuation = nextContinuation,
                            )
                        }
                    } else {
                        _uiState.update { it.copy(homeContinuation = null) }
                    }
                } catch (e: Exception) {
                    Log.e("MusicViewModel", "Error loading more home content", e)
                } finally {
                    _uiState.update { it.copy(isMoreLoading = false) }
                }
            }
        }

        private suspend fun loadDailyDiscover() {
            try {
                val seeds =
                    withContext(PerformanceDispatcher.diskIO) {
                        val history = playlistRepository.history.firstOrNull() ?: emptyList()
                        history
                            .audioMusicOnly()
                            .shuffled()
                            .take(5)
                    }

                if (seeds.isEmpty()) return

                val items = java.util.Collections.synchronizedList(mutableListOf<DailyDiscoverItem>())

                kotlinx.coroutines.coroutineScope {
                    seeds
                        .map { seed ->
                            launch(PerformanceDispatcher.networkIO) {
                                try {
                                    val related =
                                        YouTubeMusicService
                                            .getRelatedMusic(seed.videoId, 16, audioOnly = true)
                                            .audioMusicOnly()
                                    val recommendation =
                                        related
                                            .filter { it.videoId != seed.videoId }
                                            .firstOrNull { it.isAudioMusicCandidate() }
                                    if (recommendation != null) {
                                        items.add(DailyDiscoverItem(seed, recommendation))
                                    }
                                } catch (e: Exception) {
                                    Log.e("MusicViewModel", "Error fetching Daily Discover for seed ${seed.title}", e)
                                }
                            }
                        }.forEach { it.join() }
                }

                if (items.isNotEmpty()) {
                    val finalDiscover =
                        items
                            .toList()
                            .filter { it.seed.isAudioMusicCandidate() && it.recommendation.isAudioMusicCandidate() }
                            .distinctBy { it.recommendation.videoId }
                            .shuffled()
                    _uiState.update { it.copy(dailyDiscover = finalDiscover) }
                }
            } catch (e: Exception) {
                Log.e("MusicViewModel", "Error in loadDailyDiscover", e)
            }
        }
    }

data class MusicUiState(
    val sessionSeed: Long = System.currentTimeMillis(),
    val dailyDiscover: List<DailyDiscoverItem> = emptyList(),
    val onRepeatTracks: List<MusicTrack> = emptyList(), // Most recent listening history
    val rediscoverTracks: List<MusicTrack> = emptyList(), // Tail of the listening history
    val speedDialTracks: List<MusicTrack> = emptyList(), // Speed dial pool (plain order)
    val forYouTracks: List<MusicTrack> = emptyList(), // Quick Picks
    val recommendedTracks: List<MusicTrack> = emptyList(), // Recommended for you
    val listenAgain: List<MusicTrack> = emptyList(), // Listen Again
    val trendingSongs: List<MusicTrack> = emptyList(),
    val chartPlaylists: List<MusicPlaylist> = emptyList(),
    val chartArtists: List<ArtistDetails> = emptyList(),
    val chartCountryCode: String? = null,
    val newReleases: List<MusicTrack> = emptyList(),
    val musicVideos: List<MusicTrack> = emptyList(),
    val musicVideosForYou: List<MusicTrack> = emptyList(),
    val livePerformances: List<MusicTrack> = emptyList(),
    val communityPlaylists: List<CommunityMusicPlaylist> = emptyList(),
    val longListens: List<MusicTrack> = emptyList(),
    val history: List<MusicTrack> = emptyList(),
    val allSongs: List<MusicTrack> = emptyList(),
    val genreTracks: Map<String, List<MusicTrack>> = emptyMap(),
    val genres: List<String> = emptyList(),
    val featuredPlaylists: List<MusicPlaylist> = emptyList(),
    val topAlbums: List<MusicPlaylist> = emptyList(),
    val dynamicSections: List<MusicSection> = emptyList(),
    val homeChips: List<HomePage.Chip> = emptyList(),
    val selectedHomeChip: HomePage.Chip? = null,
    val explorePage: io.github.aedev.flow.innertube.pages.ExplorePage? = null,
    val moodsAndGenres: List<MoodAndGenres> = emptyList(),
    val selectedGenre: String? = null,
    val selectedFilter: String? = null,
    val isLoading: Boolean = true,
    val isSearching: Boolean = false,
    val error: String? = null,
    val downloadedTrackIds: Set<String> = emptySet(),
    val artistDetails: ArtistDetails? = null,
    val isArtistLoading: Boolean = false,
    val playlistDetails: PlaylistDetails? = null,
    val selectedPlaylist: PlaylistDetails? = null,
    val isPlaylistLoading: Boolean = false,
    val isMoreLoading: Boolean = false,
    val searchResultsArtists: List<ArtistDetails> = emptyList(),
    val homeContinuation: String? = null,
    val artistItemsPage: io.github.aedev.flow.innertube.pages.ArtistItemsPage? = null,
    val isArtistItemsLoading: Boolean = false,
    val similarToSections: List<MusicSection> = emptyList(),
    val otherPerformanceSections: List<MusicSection> = emptyList(),
    val moreFromArtistSections: List<MusicSection> = emptyList(),
)
