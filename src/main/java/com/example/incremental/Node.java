package com.example.incremental;

import java.util.List;
import java.util.Objects;

/**
 * 计算图中的一个节点。
 *
 * <p>源节点（{@link #source(String)}）没有计算函数，值由外部通过
 * {@link IncrementalEngine#setSource(String, Object)} 写入；
 * 派生节点（{@link #derived(String, List, ComputeFunction)}）声明依赖与计算函数，
 * 当依赖变脏时由引擎自动重算。
 */
public final class Node<T> {

    private final String id;
    private final List<String> dependencies;
    private final ComputeFunction<T> computeFunction;

    private Node(String id, List<String> dependencies, ComputeFunction<T> computeFunction) {
        this.id = Objects.requireNonNull(id, "id");
        this.dependencies = List.copyOf(dependencies);
        this.computeFunction = computeFunction;
    }

    /** 创建源节点（图的输入）。 */
    public static <T> Node<T> source(String id) {
        return new Node<>(id, List.of(), null);
    }

    /** 创建派生节点。 */
    public static <T> Node<T> derived(String id, List<String> dependencies, ComputeFunction<T> computeFunction) {
        Objects.requireNonNull(computeFunction, "computeFunction");
        return new Node<>(id, dependencies, computeFunction);
    }

    public String id() {
        return id;
    }

    public List<String> dependencies() {
        return dependencies;
    }

    public boolean isSource() {
        return computeFunction == null;
    }

    ComputeFunction<T> computeFunction() {
        return computeFunction;
    }
}
