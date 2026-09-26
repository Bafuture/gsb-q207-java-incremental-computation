package com.example.incremental;

import java.util.List;

/** 向计算图中加入节点时检测到循环依赖。 */
public final class CycleDetectedException extends RuntimeException {

    private final List<String> cyclePath;

    public CycleDetectedException(List<String> cyclePath) {
        super("检测到循环依赖: " + String.join(" -> ", cyclePath));
        this.cyclePath = List.copyOf(cyclePath);
    }

    /** 环路径，首尾为同一节点，例如 [a, b, c, a]。 */
    public List<String> cyclePath() {
        return cyclePath;
    }
}
