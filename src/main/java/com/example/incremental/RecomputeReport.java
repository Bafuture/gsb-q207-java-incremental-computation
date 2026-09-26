package com.example.incremental;

import java.util.List;

/**
 * 一轮重算的结果报告。
 *
 * @param recomputedNodeIds   本轮实际重算的节点 id（按拓扑序）
 * @param fullRecomputeNodes  全量重算需要计算的节点数，用于对比增量收益
 * @param computeNanos        本轮重算耗时（纳秒）
 */
public record RecomputeReport(List<String> recomputedNodeIds, int fullRecomputeNodes, long computeNanos) {

    public int recomputedCount() {
        return recomputedNodeIds.size();
    }

    /** 相对全量重算跳过的节点数。 */
    public int skippedCount() {
        return fullRecomputeNodes - recomputedCount();
    }
}
