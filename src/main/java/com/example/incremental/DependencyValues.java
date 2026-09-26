package com.example.incremental;

/** 计算函数运行时读取依赖节点值的入口。 */
@FunctionalInterface
public interface DependencyValues {

    /**
     * 读取某个直接依赖节点的当前值。
     *
     * @param dependencyId 依赖节点 id（必须是当前节点声明过的依赖）
     * @param <T>          期望的值类型
     * @return 依赖节点的最新值
     */
    @SuppressWarnings("unchecked")
    <T> T get(String dependencyId);
}
