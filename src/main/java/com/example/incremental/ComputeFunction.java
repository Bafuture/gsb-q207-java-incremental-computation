package com.example.incremental;

/** 派生节点的计算函数：根据依赖值算出本节点的新值。 */
@FunctionalInterface
public interface ComputeFunction<T> {

    T compute(DependencyValues dependencies);
}
