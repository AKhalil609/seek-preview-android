# FrameScout

**Seek-preview thumbnail generation for Android video players.**

FrameScout generates YouTube/Netflix-style scrub preview thumbnails in the background while a user watches a video. Thumbnails are extracted from the stream via HTTP range requests, stored in a compact binary cache on disk, and served back to your UI at any timestamp in milliseconds.

[![JitPack](https://jitpack.io/v/AKhalil609/FrameScout.svg)](https://jitpack.io/#AKhalil609/FrameScout)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![API](https://img.shields.io/badge/API-24%2B-brightgreen.svg)](https://android-arsenal.com/api?level=24)

---

## Features

- **Two-pass generation** — a sparse pass delivers whole-movie coverage in minutes; a dense pass fills in 30-second intervals chunk by chunk
- **Coroutine-native API** — `start`, `stop`, and `continueNextChunk` integrate cleanly with `viewModelScope` or any `CoroutineScope`
- **Resumable** — a watermark written to disk on each chunk commit means interrupted sessions pick up where they left off
- **Persistent disk cache** — compact `.nvst` binary format with LRU eviction and a configurable size budget
- **Content-identity cache keys** — keys derived from file hash, filename + size, or torrent info hash survive URL rotation (e.g. debrid-resolved links)
- **HDR / 10-bit support** — `RGBA_F16`, `RGBA_1010102`, and `HARDWARE` bitmaps are colour-managed down to `ARGB_8888` before JPEG compression
- **Pluggable grabber interface** — swap out `MmrFrameGrabber` for any custom `FrameGrabber` implementation
- **HLS / DASH auto-skip** — manifest-based streams are detected and skipped automatically; no configuration needed
- **Parallel workers** — configurable worker count with round-robin timestamp distribution for consistent early coverage
- **Frame-grab timeout** — each grab is bounded to 20 seconds via `withTimeoutOrNull` so a stalled HTTP connection never blocks a worker indefinitely

---

## Installation

Add JitPack to your root `settings.gradle.kts` (or `build.gradle`):

```kotlin
dependencyResolutionManagement {
    repositories {
        maven("https://jitpack.io")
    }
}
```

Add the dependency to your module's `build.gradle.kts`:

```kotlin
dependencies {
    implementation("com.github.AKhalil609:FrameScout:1.0.2")
}
```

---

## Quick Start

```kotlin
// 1. Create the store (once, application-scoped)
val store = SeekPreviewThumbnailStore(
    rootDir = File(context.cacheDir, "seek_previews")
)

// 2. Create the generator (once per player session)
val generator = SeekPreviewGenerator(
    store = store,
    grabberFactory = MmrFrameGrabber  // built-in MediaMetadataRetriever implementation
)

// 3. Observe state transitions and advance chunks
viewModelScope.launch {
    generator.state.collect { state ->
        when (state) {
            is SeekPreviewGenerator.State.ChunkDone -> {
                if (state.hasMoreChunks) generator.continueNextChunk(viewModelScope)
            }
            is SeekPreviewGenerator.State.BadSource -> {
                // This URL didn't work — try an alternative source
            }
            else -> Unit
        }
    }
}

// 4. Start generation when the stream and its duration are known
val key = SeekPreviewCacheKey.compute(
    SeekPreviewCacheKey.Input(
        videoHash  = null,           // OpenSubtitles hash if available
        filename   = "Movie.2024.1080p.BluRay.x264.mp4",
        videoSize  = 8_589_934_592L, // bytes
        infoHash   = null,
        fileIdx    = null,
        url        = streamUrl
    )
)

generator.start(
    input = SeekPreviewGenerator.Input(
        key          = key,
        url          = streamUrl,
        headers      = mapOf("User-Agent" to "MyApp/1.0"),
        durationMs   = 8_160_000L,   // 2h 16m
        generationType = SeekPreviewGenerationType.DETAILED
    ),
    scope = viewModelScope
)

// 5. Look up the nearest thumbnail for any position (UI thread safe)
val jpeg: ByteArray? = generator.nearestJpeg(positionMs)
```

---

## Configuration

Pass a `SeekPreviewGenerator.Config` to customise generation behaviour:

```kotlin
val generator = SeekPreviewGenerator(
    store          = store,
    grabberFactory = MmrFrameGrabber,
    config = SeekPreviewGenerator.Config(
        widthPx              = 160,       // thumbnail width
        heightPx             = 90,        // thumbnail height (16:9 at 160 wide)
        intervalMs           = 30_000,    // dense: one frame every 30 s
        sparseIntervalMs     = 180_000,   // sparse: one frame every 3 min
        jpegQuality          = 60,        // JPEG compression quality (0–100)
        nearestMaxDeltaMs    = 120_000L,  // max gap for nearestJpeg() lookups
        workerCount          = 2,         // parallel MediaMetadataRetriever connections
        chunkFraction        = 0.25,      // dense pass: 25% of duration per chunk
        shortVideoThresholdMs = 5 * 60_000L, // content under 5 min = single chunk
        interGrabDelayMs     = 500L       // pause between grabs per worker
    )
)
```

| Parameter | Default | Description |
|---|---|---|
| `widthPx` | `160` | Thumbnail width in pixels |
| `heightPx` | `90` | Thumbnail height in pixels |
| `intervalMs` | `30 000` | Frame interval for the dense pass (ms) |
| `sparseIntervalMs` | `180 000` | Frame interval for the sparse pass (ms) |
| `jpegQuality` | `60` | JPEG quality (0 = smallest, 100 = best) |
| `nearestMaxDeltaMs` | `120 000` | Maximum distance for `nearestJpeg()` to return a result |
| `workerCount` | `2` | Number of parallel grabber workers |
| `chunkFraction` | `0.25` | Fraction of total duration per dense chunk (4 chunks at 25% each) |
| `shortVideoThresholdMs` | `300 000` | Content below this threshold is generated in a single chunk |
| `interGrabDelayMs` | `500` | Delay between grabs per worker — prevents the hardware decoder from being monopolised |

---

## Generation Types

| Type | Description | When to use |
|---|---|---|
| `SPARSE` | One frame every `sparseIntervalMs` (default 3 min) across the whole file. Fast — a 2-hour movie produces ~40 frames. | Low-bandwidth or background use. Gives rough full-movie coverage immediately. |
| `DETAILED` | Sparse pass first, then fills in at `intervalMs` (default 30 s) chunk by chunk. | Default for streaming apps where detailed scrubbing is expected. |

---

## State Machine

```
Idle
 └─► Probing           (checking source duration)
      ├─► BadSource     (source too short or all frames returned null — retry with next URL)
      └─► Generating    (framesDone, framesTotal, chunkIndex, totalChunks, isSparse)
           ├─► ChunkDone  (completedChunkIndex, hasMoreChunks, generatedThroughMs, isSparse)
           │    └─► [call continueNextChunk() to advance]
           ├─► Done
           ├─► Unsupported  (HLS / DASH / torrent stream detected)
           └─► Failed       (invalid duration or unexpected error)
```

`BadSource` is emitted in two cases:
- The source duration is under 60 seconds (debrid "not cached yet" error clip).
- The sparse pass completed with zero frames (codec not supported for HTTP seeking on this device — e.g. HEVC on some hardware).

In both cases, add `state.triedUrl` to an exclusion set and retry `start()` with an alternative source URL.

---

## Cache Keys

Debrid-resolved URLs are time-limited and IP-locked — using a URL as a cache key would invalidate the cache every session. `SeekPreviewCacheKey.compute()` derives a stable key from content identity instead:

| Priority | Input | Key prefix |
|---|---|---|
| 1 (best) | `videoHash` (OpenSubtitles hash) | `vh-` |
| 2 | `filename + videoSize` | `fs-` |
| 3 | `infoHash + fileIdx` (torrent) | `ih-` |
| 4 (fallback) | URL | `u-` |

```kotlin
val key = SeekPreviewCacheKey.compute(
    SeekPreviewCacheKey.Input(
        videoHash = openSubtitlesHash, // most stable — same bytes = same hash
        filename  = "Movie.2024.1080p.mp4",
        videoSize = fileSizeBytes,
        infoHash  = null,
        fileIdx   = null,
        url       = streamUrl          // fallback only
    )
)
```

---

## Custom FrameGrabber

The built-in `MmrFrameGrabber` uses `MediaMetadataRetriever` for MP4 sources over HTTP. To support other sources or decoders, implement `FrameGrabber`:

```kotlin
class MyCustomGrabber : FrameGrabber {

    override fun open(url: String, headers: Map<String, String>) {
        // Bind to the source
    }

    override fun grab(tsMs: Long, widthPx: Int, heightPx: Int, jpegQuality: Int): ByteArray? {
        // Extract and return a JPEG frame, or null on failure
    }

    override fun sourceDurationMs(): Long? {
        // Return container duration for the probe step
    }

    override fun close() {
        // Release resources
    }
}

// Pass a factory to the generator
val generator = SeekPreviewGenerator(
    store          = store,
    grabberFactory = FrameGrabberFactory { MyCustomGrabber() }
)
```

Each worker receives its own grabber instance from the factory. Implementations do not need to be thread-safe.

---

## Requirements

- **Minimum SDK**: API 24 (Android 7.0)
- **Language**: Kotlin
- **Dependencies**: `kotlinx-coroutines-android`
- Frames are extracted using `android.media.MediaMetadataRetriever` — no native libraries or additional decoders required

---

## Contributing

Bug reports and pull requests are welcome.

1. **Open an issue first** for anything beyond a small bug fix — describe the problem or feature before writing code.
2. Fork the repo and create a branch from `main`.
3. Write or update tests for any changed behaviour.
4. Open a pull request. Keep the scope focused — one change per PR.

For bug reports, please include the Android API level, device SoC (e.g. Amlogic, Qualcomm), and the stream container / codec you were using.

---

## License

```
Copyright 2026 AKK

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
