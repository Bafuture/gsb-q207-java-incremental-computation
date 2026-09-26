package com.example.incremental;

/**
 * 引擎统计快照。
 *
 * @param totalNodes           图中节点总数（含源节点）
 * @param lastRecomputedNodes  本轮（最近一次 recompute）实际重算的节点数
 * @param fullRecomputeNodes   全量重算需要计算的节点数（所有派生节点），用于与本轮重算数对比
 * @param cacheHits            取值缓存命中次数（无变化时 get 不触发计算）
 * @param totalComputeNanos    所有重算的累计耗时（纳秒）
 */
public record EngineStats(
        int totalNodes,
        int lastRecomputedNodes,
        int fullRecomputeNodes,
        long cacheHits,
        long totalComputeNanos) {

    public double totalComputeMillis() {
        return totalComputeNanos / 1_000_000.0;
    }
}
