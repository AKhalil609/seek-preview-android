package io.framescout

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.coroutineContext
import kotlin.math.ceil

/**
 * Orchestrates seek-preview thumbnail generation for a single stream,
 * writing results into a [SeekPreviewThumbnailStore].
 *
 * The work is split into chunks covering successive fractions of the
 * total duration (25% by default). Videos shorter than
 * [Config.shortVideoThresholdMs] are generated in a single chunk.
 *
 * Each chunk is executed by up to [Config.workerCount] parallel workers,
 * each owning its own [FrameGrabber]. Timestamps within a chunk are
 * assigned round-robin so early positions in the chunk are grabbed
 * soonest regardless of worker count.
 *
 * The caller is responsible for advancing chunks via [continueNextChunk]
 * when a [State.ChunkDone] state is observed.
 */
class SeekPreviewGenerator(
    private val store: SeekPreviewThumbnailStore,
    private val grabberFactory: FrameGrabberFactory,
    private val workDispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val config: Config = Config()
) {

    data class Config(
        val widthPx: Int = 160,
        val heightPx: Int = 90,
        val intervalMs: Int = 30_000,
        val sparseIntervalMs: Int = 180_000,
        val jpegQuality: Int = 60,
        val nearestMaxDeltaMs: Long = 120_000L,
        val workerCount: Int = 2,
        val chunkFraction: Double = 0.25,
        val shortVideoThresholdMs: Long = 5 * 60_000L,
        val interGrabDelayMs: Long = 500L
    )

    data class Input(
        val key: String,
        val url: String,
        val headers: Map<String, String>,
        val durationMs: Long,
        val mimeTypeHint: String? = null,
        val generationType: SeekPreviewGenerationType = SeekPreviewGenerationType.SPARSE
    )

    sealed class State {
        data object Idle : State()
        data object Probing : State()
        data class Generating(
            val framesDone: Int,
            val framesTotal: Int,
            val chunkIndex: Int,
            val totalChunks: Int,
            val isSparse: Boolean = false
        ) : State()
        data class ChunkDone(
            val completedChunkIndex: Int,
            val totalChunks: Int,
            val hasMoreChunks: Boolean,
            val generatedThroughMs: Long,
            val isSparse: Boolean = false
        ) : State()
        data object Done : State()
        data object Unsupported : State()
        data class Failed(val message: String) : State()
        data class BadSource(val triedUrl: String) : State()
    }

    private val _state = MutableStateFlow<State>(State.Idle)
    val state: StateFlow<State> = _state.asStateFlow()

    private val _cachedFractions = MutableStateFlow(floatArrayOf())
    val cachedFractions: StateFlow<FloatArray> = _cachedFractions.asStateFlow()

    private val lock = Any()
    private var currentEntry: SeekPreviewThumbnailStore.MovieEntry? = null
    private var currentInput: Input? = null
    private var currentChunkRanges: List<LongRange> = emptyList()
    private var currentAllTimestamps: List<Long> = emptyList()
    private var nextChunkIndex: Int = 0
    private var totalChunksWithPasses: Int = 0
    private var job: Job? = null

    fun start(input: Input, scope: CoroutineScope): Job {
        synchronized(lock) {
            job?.cancel()
            closeCurrentEntryLocked()
            currentInput = input
            currentChunkRanges = emptyList()
            currentAllTimestamps = emptyList()
            nextChunkIndex = 0
            totalChunksWithPasses = 0
            _cachedFractions.value = floatArrayOf()
            val launched = scope.launch(workDispatcher) {
                runInitialAndFirstChunk(input)
            }
            job = launched
            return launched
        }
    }

    fun continueNextChunk(scope: CoroutineScope): Job? {
        synchronized(lock) {
            if (job?.isActive == true) return null
            val input = currentInput ?: return null
            val index = nextChunkIndex
            if (index >= totalChunksWithPasses) return null
            val launched = scope.launch(workDispatcher) {
                runChunk(input, index)
            }
            job = launched
            return launched
        }
    }

    fun stop() {
        synchronized(lock) {
            job?.cancel()
            job = null
            closeCurrentEntryLocked()
            currentInput = null
            currentChunkRanges = emptyList()
            currentAllTimestamps = emptyList()
            nextChunkIndex = 0
            totalChunksWithPasses = 0
            _state.value = State.Idle
            _cachedFractions.value = floatArrayOf()
        }
    }

    fun nearestJpeg(tsMs: Long): ByteArray? {
        val entry = synchronized(lock) { currentEntry } ?: return null
        return entry.nearest(tsMs, config.nearestMaxDeltaMs)
    }

    private suspend fun runInitialAndFirstChunk(input: Input) {
        if (!isSupported(input.mimeTypeHint, input.url)) {
            _state.value = State.Unsupported
            return
        }
        if (input.durationMs <= 0L) {
            _state.value = State.Failed("invalid duration")
            return
        }
        _state.value = State.Probing

        val probedDuration = probeSourceDuration(input)
        if (probedDuration != null && probedDuration < SHORT_SOURCE_THRESHOLD_MS) {
            _state.value = State.BadSource(triedUrl = input.url)
            return
        }

        val entry = store.open(
            key = input.key,
            width = config.widthPx,
            height = config.heightPx,
            intervalMs = config.intervalMs,
            durationMs = input.durationMs
        )
        val allTs = buildTimestamps(input.durationMs, config.intervalMs.toLong())
        val ranges = computeChunkRanges(input.durationMs, config)

        val hasSparsePass = config.sparseIntervalMs > config.intervalMs
        val total = ranges.size + (if (hasSparsePass) 1 else 0)

        synchronized(lock) {
            currentEntry = entry
            currentAllTimestamps = allTs
            currentChunkRanges = ranges
            totalChunksWithPasses = total
        }

        if (entry.size() > 0) updateCachedFractions(entry, input.durationMs)

        if (entry.isCompleteThrough(input.durationMs) || allTs.isEmpty() || ranges.isEmpty()) {
            _state.value = State.Done
            synchronized(lock) { nextChunkIndex = total }
            return
        }
        runChunk(input, chunkIndex = 0)
    }

    private suspend fun runChunk(input: Input, chunkIndex: Int) {
        val entry = synchronized(lock) { currentEntry } ?: return
        val ranges = synchronized(lock) { currentChunkRanges }
        val allTs = synchronized(lock) { currentAllTimestamps }
        val total = synchronized(lock) { totalChunksWithPasses }
        if (chunkIndex >= total) return

        val hasSparsePass = config.sparseIntervalMs > config.intervalMs
        val isSparse = hasSparsePass && chunkIndex == 0

        if (!isSparse && input.generationType == SeekPreviewGenerationType.SPARSE) {
            _state.value = State.Done
            synchronized(lock) { nextChunkIndex = total }
            return
        }

        val chunkTs = if (isSparse) {
            val step = (config.sparseIntervalMs / config.intervalMs).coerceAtLeast(1)
            allTs.filterIndexed { i, _ -> i % step == 0 }
        } else {
            val rangeIndex = if (hasSparsePass) chunkIndex - 1 else chunkIndex
            if (rangeIndex !in ranges.indices) return
            val range = ranges[rangeIndex]
            allTs.filter { it in range }
        }

        val missing = chunkTs.filterNot { entry.hasTimestamp(it) }
        val totalInChunk = chunkTs.size
        val alreadyDone = totalInChunk - missing.size
        val doneCounter = AtomicInteger(alreadyDone)
        emitGenerating(doneCounter.get(), totalInChunk, chunkIndex, total, isSparse)

        val shards = shardTimestamps(missing, config.workerCount)
        val onFrameDone: () -> Unit = {
            val d = doneCounter.incrementAndGet()
            emitGenerating(d, totalInChunk, chunkIndex, total, isSparse)
        }

        try {
            coroutineScope {
                for (shard in shards) {
                    if (shard.isEmpty()) continue
                    launch(workDispatcher) { runWorker(input, shard, entry, onFrameDone) }
                }
            }
        } finally {
            if (!isSparse) {
                val watermark = lastContiguous(entry, allTs)
                runCatching { entry.commit(watermark) }
            } else {
                runCatching { entry.commit(entry.generatedThroughMs) }
            }
            updateCachedFractions(entry, input.durationMs)
        }

        if (isSparse && entry.size() == 0) {
            _state.value = State.BadSource(triedUrl = input.url)
            synchronized(lock) { nextChunkIndex = total }
            return
        }

        synchronized(lock) { nextChunkIndex = chunkIndex + 1 }

        if (!coroutineContext.isActive) return

        val hasMore = chunkIndex + 1 < total
        _state.value = if (!hasMore) {
            State.Done
        } else {
            State.ChunkDone(
                completedChunkIndex = chunkIndex,
                totalChunks = total,
                hasMoreChunks = true,
                generatedThroughMs = entry.generatedThroughMs,
                isSparse = isSparse
            )
        }
    }

    private suspend fun runWorker(
        input: Input,
        timestamps: List<Long>,
        entry: SeekPreviewThumbnailStore.MovieEntry,
        onFrameDone: () -> Unit
    ) {
        val grabber = grabberFactory.create()
        try {
            val opened = try {
                grabber.open(input.url, input.headers)
                true
            } catch (_: Throwable) {
                repeat(timestamps.size) { onFrameDone() }
                false
            }
            if (!opened) return
            for (ts in timestamps) {
                if (!coroutineContext.isActive) break
                val jpeg = withTimeoutOrNull(FRAME_GRAB_TIMEOUT_MS) {
                    runInterruptible {
                        runCatching {
                            grabber.grab(ts, config.widthPx, config.heightPx, config.jpegQuality)
                        }.getOrNull()
                    }
                }
                if (jpeg != null) entry.put(ts, jpeg)
                onFrameDone()
                if (config.interGrabDelayMs > 0L) delay(config.interGrabDelayMs)
            }
        } finally {
            runCatching { grabber.close() }
        }
    }

    private fun updateCachedFractions(entry: SeekPreviewThumbnailStore.MovieEntry, durationMs: Long) {
        if (durationMs <= 0L) return
        val keys = entry.cachedTimestampKeys()
        _cachedFractions.value = FloatArray(keys.size) { i ->
            (keys[i].toFloat() / durationMs.toFloat()).coerceIn(0f, 1f)
        }
    }

    private suspend fun probeSourceDuration(input: Input): Long? =
        probeDurationMs(input.url, input.headers)

    suspend fun probeDurationMs(url: String, headers: Map<String, String>): Long? {
        val grabber = grabberFactory.create()
        return try {
            runInterruptible {
                grabber.open(url, headers)
                grabber.sourceDurationMs()
            }
        } catch (_: Throwable) {
            null
        } finally {
            runCatching { grabber.close() }
        }
    }

    private fun emitGenerating(done: Int, total: Int, chunkIndex: Int, totalChunks: Int, isSparse: Boolean) {
        _state.value = State.Generating(
            framesDone = done,
            framesTotal = total,
            chunkIndex = chunkIndex,
            totalChunks = totalChunks,
            isSparse = isSparse
        )
    }

    private fun closeCurrentEntryLocked() {
        val entry = currentEntry ?: return
        currentEntry = null
        runCatching { entry.close() }
    }

    companion object {
        private const val FRAME_GRAB_TIMEOUT_MS = 20_000L
        internal const val SHORT_SOURCE_THRESHOLD_MS = 60_000L

        internal fun buildTimestamps(durationMs: Long, intervalMs: Long): List<Long> {
            if (durationMs <= 0L || intervalMs <= 0L) return emptyList()
            val list = ArrayList<Long>()
            var t = 0L
            while (t <= durationMs) {
                list.add(t)
                t += intervalMs
            }
            return list
        }

        internal fun lastContiguous(
            entry: SeekPreviewThumbnailStore.MovieEntry,
            timestamps: List<Long>
        ): Long {
            var last = 0L
            var any = false
            for (ts in timestamps) {
                if (entry.hasTimestamp(ts)) {
                    last = ts
                    any = true
                } else {
                    break
                }
            }
            return if (any) last else 0L
        }

        internal fun isSupported(mimeHint: String?, url: String): Boolean {
            val mime = mimeHint?.trim()?.lowercase().orEmpty()
            if (mime.startsWith("application/vnd.apple.mpegurl") ||
                mime.startsWith("application/x-mpegurl") ||
                mime.startsWith("application/dash+xml")
            ) return false
            val path = url.substringBefore('?').lowercase()
            return !path.endsWith(".m3u8") && !path.endsWith(".mpd")
        }

        internal fun computeChunkRanges(durationMs: Long, cfg: Config): List<LongRange> {
            if (durationMs <= 0L) return emptyList()
            if (durationMs < cfg.shortVideoThresholdMs) return listOf(0L..durationMs)
            val n = maxOf(1, ceil(1.0 / cfg.chunkFraction).toInt())
            return (0 until n).map { i ->
                val start = durationMs * i / n
                val end = if (i == n - 1) durationMs
                else (durationMs * (i + 1) / n) - 1L
                start..end
            }
        }

        internal fun shardTimestamps(timestamps: List<Long>, shards: Int): List<List<Long>> {
            if (timestamps.isEmpty()) return emptyList()
            if (shards <= 1) return listOf(timestamps)
            val result = Array(shards) { mutableListOf<Long>() }
            for ((i, ts) in timestamps.withIndex()) result[i % shards].add(ts)
            return result.map { it.toList() }
        }
    }
}
