package com.nuvio.seekpreview

enum class SeekPreviewGenerationType {
    /** One frame every 3 minutes for the whole movie. Fast and lightweight. */
    SPARSE,
    /** One frame every 3 minutes, then fills gaps with one frame every 30 seconds. Detailed but slower. */
    DETAILED
}
