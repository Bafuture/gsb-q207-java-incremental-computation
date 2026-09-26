package com.example.incremental;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 增量计算引擎：节点构成有向无环图（DAG），源数据变化时沿依赖链传播脏标记，
 * 一轮重算中每个受影响节点只计算一次，且互不依赖的节点并行计算。
 *
 * <p>节点可按任意顺序注册（允许前向引用），每次注册都会做环检测；
 * 依赖是否真实存在会在重算时校验。
 */
public final class IncrementalEngine implements AutoCloseable {

    private final Map<String, Node<?>> nodes = new LinkedHashMap<>();
    private final Map<String, Set<String>> dependents = new HashMap<>();
    private final Map<String, Object> values = new ConcurrentHashMap<>();
    private final Set<String> dirty = new LinkedHashSet<>();

    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong totalComputeNanos = new AtomicLong();
    private volatile int lastRecomputedCount;

    private final ExecutorService executor;

    public IncrementalEngine() {
        this(Math.max(2, Runtime.getRuntime().availableProcessors()));
    }

    public IncrementalEngine(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism 必须 >= 1");
        }
        this.executor = Executors.newFixedThreadPool(parallelism, runnable -> {
            Thread thread = new Thread(runnable, "incremental-engine-worker");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** 注册节点。若加入后图中出现循环依赖，抛出 {@link CycleDetectedException}（含环路径）。 */
    public synchronized <T> void addNode(Node<T> node) {
        Objects.requireNonNull(node, "node");
        if (nodes.containsKey(node.id())) {
            throw new IllegalArgumentException("节点已存在: " + node.id());
        }
        nodes.put(node.id(), node);
        for (String dependency : node.dependencies()) {
            dependents.computeIfAbsent(dependency, key -> new LinkedHashSet<>()).add(node.id());
        }
        List<String> cycle = findCycleFrom(node.id());
        if (cycle != null) {
            nodes.remove(node.id());
            for (String dependency : node.dependencies()) {
                dependents.get(dependency).remove(node.id());
            }
            throw new CycleDetectedException(cycle);
        }
        if (!node.isSource()) {
            dirty.add(node.id());
        }
    }

    /** 写入源节点的新值，并沿依赖链把下游全部标记为脏。 */
    public synchronized void setSource(String id, Object value) {
        Node<?> node = requireNode(id);
        if (!node.isSource()) {
            throw new IllegalArgumentException("节点 " + id + " 不是源节点，不能直接赋值");
        }
        values.put(id, value);
        propagateDirty(id);
    }

    /** 手动使某个节点失效（连同其全部下游）。 */
    public synchronized void invalidate(String id) {
        Node<?> node = requireNode(id);
        if (!node.isSource()) {
            dirty.add(id);
        }
        propagateDirty(id);
    }

    /**
     * 读取节点值。无变化（未脏且已有缓存值）时直接返回缓存并计入缓存命中；
     * 否则先触发一轮增量重算再返回。
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String id) {
        synchronized (this) {
            requireNode(id);
            if (!dirty.contains(id) && values.containsKey(id)) {
                cacheHits.incrementAndGet();
                return (T) values.get(id);
            }
        }
        recompute();
        Object value = values.get(id);
        if (value == null && !values.containsKey(id)) {
            throw new IllegalStateException("节点 " + id + " 没有可用值（源节点尚未赋值）");
        }
        return (T) value;
    }

    /**
     * 执行一轮增量重算：同一轮内多个上游变化会被合并，每个脏节点只计算一次；
     * 同一拓扑层内互不依赖的节点并行计算。
     */
    public RecomputeReport recompute() {
        List<String> batch;
        synchronized (this) {
            batch = new ArrayList<>(dirty);
            dirty.clear();
        }
        if (batch.isEmpty()) {
            return new RecomputeReport(List.of(), fullRecomputeNodes(), 0L);
        }
        long started = System.nanoTime();
        List<String> recomputed = recomputeBatch(batch);
        long elapsed = System.nanoTime() - started;
        totalComputeNanos.addAndGet(elapsed);
        lastRecomputedCount = recomputed.size();
        return new RecomputeReport(recomputed, fullRecomputeNodes(), elapsed);
    }

    private List<String> recomputeBatch(List<String> batch) {
        Set<String> pending = new LinkedHashSet<>(batch);
        for (String id : pending) {
            for (String dependency : nodes.get(id).dependencies()) {
                if (!nodes.containsKey(dependency)) {
                    throw new IllegalArgumentException(
                            "节点 " + id + " 依赖了不存在的节点: " + dependency);
                }
            }
        }
        List<String> recomputed = new ArrayList<>();
        while (!pending.isEmpty()) {
            List<String> ready = new ArrayList<>();
            for (String id : pending) {
                boolean dependenciesReady = true;
                for (String dependency : nodes.get(id).dependencies()) {
                    if (pending.contains(dependency)) {
                        dependenciesReady = false;
                        break;
                    }
                }
                if (dependenciesReady) {
                    ready.add(id);
                }
            }
            if (ready.isEmpty()) {
                throw new CycleDetectedException(new ArrayList<>(pending));
            }
            List<Future<?>> futures = new ArrayList<>();
            for (String id : ready) {
                futures.add(executor.submit(() -> computeOne(id)));
            }
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("重算被中断", e);
                } catch (ExecutionException e) {
                    throw new IllegalStateException("节点重算失败: " + e.getCause(), e.getCause());
                }
            }
            pending.removeAll(ready);
            recomputed.addAll(ready);
        }
        return recomputed;
    }

    @SuppressWarnings("unchecked")
    private <T> void computeOne(String id) {
        Node<T> node = (Node<T>) nodes.get(id);
        DependencyValues dependencyValues = new DependencyValues() {
            @Override
            public <V> V get(String dependencyId) {
                if (!node.dependencies().contains(dependencyId)) {
                    throw new IllegalArgumentException(
                            "节点 " + id + " 未声明依赖: " + dependencyId);
                }
                return (V) values.get(dependencyId);
            }
        };
        T result = node.computeFunction().compute(dependencyValues);
        values.put(id, result);
    }

    private void propagateDirty(String id) {
        Deque<String> queue = new ArrayDeque<>();
        queue.add(id);
        Set<String> visited = new HashSet<>();
        visited.add(id);
        while (!queue.isEmpty()) {
            String current = queue.poll();
            for (String dependent : dependents.getOrDefault(current, Set.of())) {
                if (visited.add(dependent)) {
                    if (!nodes.get(dependent).isSource()) {
                        dirty.add(dependent);
                    }
                    queue.add(dependent);
                }
            }
        }
    }

    private List<String> findCycleFrom(String startId) {
        Map<String, String> parent = new HashMap<>();
        Set<String> onStack = new HashSet<>();
        Set<String> done = new HashSet<>();
        Deque<String> stack = new ArrayDeque<>();
        stack.push(startId);
        onStack.add(startId);
        while (!stack.isEmpty()) {
            String current = stack.peek();
            boolean pushedChild = false;
            for (String dependency : nodes.get(current).dependencies()) {
                if (!nodes.containsKey(dependency)) {
                    continue;
                }
                if (onStack.contains(dependency)) {
                    List<String> cycle = new ArrayList<>();
                    String cursor = current;
                    while (true) {
                        cycle.add(0, cursor);
                        if (cursor.equals(dependency)) {
                            break;
                        }
                        cursor = parent.get(cursor);
                    }
                    cycle.add(dependency);
                    return cycle;
                }
                if (!done.contains(dependency)) {
                    parent.put(dependency, current);
                    stack.push(dependency);
                    onStack.add(dependency);
                    pushedChild = true;
                    break;
                }
            }
            if (!pushedChild) {
                stack.pop();
                onStack.remove(current);
                done.add(current);
            }
        }
        return null;
    }

    private Node<?> requireNode(String id) {
        Node<?> node = nodes.get(id);
        if (node == null) {
            throw new IllegalArgumentException("未知节点: " + id);
        }
        return node;
    }

    private int fullRecomputeNodes() {
        int count = 0;
        for (Node<?> node : nodes.values()) {
            if (!node.isSource()) {
                count++;
            }
        }
        return count;
    }

    /** 当前统计快照。 */
    public synchronized EngineStats stats() {
        return new EngineStats(
                nodes.size(),
                lastRecomputedCount,
                fullRecomputeNodes(),
                cacheHits.get(),
                totalComputeNanos.get());
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
