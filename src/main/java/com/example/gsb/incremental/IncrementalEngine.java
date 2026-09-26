package com.example.gsb.incremental;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * An incremental computation engine.
 *
 * <p>Nodes form a directed acyclic graph. Source nodes hold externally supplied values;
 * derived nodes compute their value from their dependencies. When a source changes, dirty
 * flags propagate along the dependency chain and only affected nodes are recomputed, with
 * independent nodes recomputed in parallel. Clean values are served from cache.
 */
public final class IncrementalEngine implements AutoCloseable {

    private static final class NodeDef {
        final String name;
        final List<String> dependencies;
        final Function<List<Object>, Object> compute; // null for source nodes
        volatile Object value;
        volatile boolean dirty;

        NodeDef(String name, List<String> dependencies, Function<List<Object>, Object> compute, Object value) {
            this.name = name;
            this.dependencies = dependencies;
            this.compute = compute;
            this.value = value;
        }

        boolean isSource() {
            return compute == null;
        }
    }

    private final Map<String, NodeDef> nodes = new LinkedHashMap<>();
    private final Map<String, List<String>> dependents = new HashMap<>();
    private final ExecutorService pool;
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong totalComputeNanos = new AtomicLong();
    private volatile int lastRoundRecomputed;

    public IncrementalEngine() {
        this(Runtime.getRuntime().availableProcessors());
    }

    public IncrementalEngine(int parallelism) {
        this.pool = Executors.newFixedThreadPool(Math.max(1, parallelism));
    }

    /** Registers a source node with an initial value. */
    public synchronized IncrementalEngine addSource(String name, Object initialValue) {
        requireAbsent(name);
        nodes.put(name, new NodeDef(name, List.of(), null, initialValue));
        return this;
    }

    /**
     * Registers a derived node. Dependencies may reference nodes not yet registered
     * (they must exist before any recompute runs).
     *
     * @throws CycleDetectedException if the new node would close a dependency cycle
     */
    public synchronized IncrementalEngine addNode(
            String name, List<String> dependencies, Function<List<Object>, Object> compute) {
        requireAbsent(name);
        Objects.requireNonNull(compute, "compute");
        List<String> deps = List.copyOf(dependencies);
        if (deps.contains(name)) {
            throw new CycleDetectedException(List.of(name, name));
        }
        NodeDef node = new NodeDef(name, deps, compute, null);
        node.dirty = true; // never computed yet
        nodes.put(name, node);
        for (String dep : deps) {
            dependents.computeIfAbsent(dep, k -> new ArrayList<>()).add(name);
        }
        // The graph was acyclic before, so any new cycle must pass through this node:
        // check whether this node is reachable from any of its dependencies.
        for (String dep : deps) {
            List<String> path = findDependencyPath(dep, name);
            if (path != null) {
                nodes.remove(name);
                for (String d : deps) {
                    dependents.get(d).remove(name);
                }
                List<String> cycle = new ArrayList<>();
                cycle.add(name);
                cycle.addAll(path);
                throw new CycleDetectedException(cycle);
            }
        }
        return this;
    }

    /** Updates a source value and propagates dirty flags to all transitive dependents. */
    public synchronized void set(String name, Object value) {
        NodeDef node = requireNode(name);
        if (!node.isSource()) {
            throw new IllegalArgumentException("Only source nodes can be set: " + name);
        }
        if (Objects.equals(node.value, value)) {
            return; // no change -> no dirty propagation
        }
        node.value = value;
        Deque<String> queue = new ArrayDeque<>();
        queue.add(name);
        while (!queue.isEmpty()) {
            for (String dependent : dependents.getOrDefault(queue.poll(), List.of())) {
                NodeDef d = nodes.get(dependent);
                if (!d.dirty) {
                    d.dirty = true;
                    queue.add(dependent);
                }
            }
        }
    }

    /**
     * Returns the node's value. Triggers a recompute round for dirty nodes if needed;
     * a clean value is served from cache and counted as a cache hit.
     */
    @SuppressWarnings("unchecked")
    public synchronized <T> T get(String name) {
        NodeDef node = requireNode(name);
        boolean servedFromCache = !node.dirty;
        recomputeDirty();
        if (servedFromCache) {
            cacheHits.incrementAndGet();
        }
        return (T) node.value;
    }

    /** Returns a snapshot of engine statistics. */
    public synchronized EngineStats stats() {
        int derived = 0;
        for (NodeDef n : nodes.values()) {
            if (!n.isSource()) {
                derived++;
            }
        }
        return new EngineStats(nodes.size(), derived, lastRoundRecomputed,
                cacheHits.get(), totalComputeNanos.get());
    }

    /**
     * Recomputes every dirty derived node exactly once, level by level in topological
     * order; nodes within the same level are independent and run in parallel.
     */
    private void recomputeDirty() {
        Set<String> dirty = new LinkedHashSet<>();
        for (NodeDef n : nodes.values()) {
            if (n.dirty && !n.isSource()) {
                dirty.add(n.name);
            }
        }
        if (dirty.isEmpty()) {
            return;
        }
        lastRoundRecomputed = dirty.size();

        // Group dirty nodes by topological level: level = 1 + max(level of dependencies).
        Map<String, Integer> levels = new HashMap<>();
        for (String name : dirty) {
            levelOf(name, levels);
        }
        Map<Integer, List<String>> byLevel = new TreeMap<>();
        for (String name : dirty) {
            byLevel.computeIfAbsent(levels.get(name), k -> new ArrayList<>()).add(name);
        }

        for (List<String> levelNodes : byLevel.values()) {
            List<Future<?>> futures = new ArrayList<>();
            for (String name : levelNodes) {
                NodeDef node = nodes.get(name);
                futures.add(pool.submit(() -> computeNode(node)));
            }
            for (Future<?> future : futures) {
                await(future);
            }
        }
    }

    private void computeNode(NodeDef node) {
        List<Object> inputs = new ArrayList<>(node.dependencies.size());
        for (String dep : node.dependencies) {
            NodeDef d = nodes.get(dep);
            if (d == null) {
                throw new IllegalStateException(
                        "Node '" + node.name + "' depends on undefined node '" + dep + "'");
            }
            inputs.add(d.value);
        }
        long start = System.nanoTime();
        Object result = node.compute.apply(inputs);
        totalComputeNanos.addAndGet(System.nanoTime() - start);
        node.value = result;
        node.dirty = false;
    }

    private int levelOf(String name, Map<String, Integer> memo) {
        Integer cached = memo.get(name);
        if (cached != null) {
            return cached;
        }
        NodeDef node = nodes.get(name);
        int level = 0;
        if (node != null) {
            for (String dep : node.dependencies) {
                level = Math.max(level, levelOf(dep, memo) + 1);
            }
        }
        memo.put(name, level);
        return level;
    }

    /** Finds a dependency path from {@code from} to {@code target}, or null if none exists. */
    private List<String> findDependencyPath(String from, String target) {
        return findDependencyPath(from, target, new LinkedHashMap<>());
    }

    private List<String> findDependencyPath(String current, String target, Map<String, Boolean> visited) {
        if (current.equals(target)) {
            return new ArrayList<>(List.of(current));
        }
        if (visited.put(current, Boolean.TRUE) != null) {
            return null;
        }
        NodeDef node = nodes.get(current);
        if (node == null) {
            return null;
        }
        for (String dep : node.dependencies) {
            List<String> sub = findDependencyPath(dep, target, visited);
            if (sub != null) {
                sub.add(0, current);
                return sub;
            }
        }
        return null;
    }

    private void requireAbsent(String name) {
        if (nodes.containsKey(Objects.requireNonNull(name, "name"))) {
            throw new IllegalArgumentException("Duplicate node: " + name);
        }
    }

    private NodeDef requireNode(String name) {
        NodeDef node = nodes.get(name);
        if (node == null) {
            throw new IllegalArgumentException("Unknown node: " + name);
        }
        return node;
    }

    private static void await(Future<?> future) {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while recomputing", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Node computation failed", e.getCause());
        }
    }

    @Override
    public void close() {
        pool.shutdown();
    }
}
