package com.example.gsb.incremental;

/**
 * Immutable snapshot of engine statistics.
 *
 * @param totalNodes           total number of registered nodes (sources + derived)
 * @param derivedNodes         number of derived (computed) nodes; a full recompute would recompute all of them
 * @param lastRoundRecomputed  number of nodes recomputed in the most recent recompute round
 * @param cacheHits            cumulative number of get() calls served from cache without computation
 * @param totalComputeNanos    cumulative wall time spent inside compute functions, in nanoseconds
 */
public record EngineStats(
        int totalNodes,
        int derivedNodes,
        int lastRoundRecomputed,
        long cacheHits,
        long totalComputeNanos) {

    /** Number of nodes a full (non-incremental) recompute would have to recompute. */
    public int fullRecomputeCount() {
        return derivedNodes;
    }

    /** Ratio of work saved by incremental recompute in the last round, in [0, 1]. */
    public double lastRoundSavingsRatio() {
        if (derivedNodes == 0) {
            return 0.0;
        }
        return 1.0 - (double) lastRoundRecomputed / derivedNodes;
    }
}
