package com.example.gsb.incremental;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class IncrementalEngineTest {

    private static int sum(List<Object> inputs) {
        return inputs.stream().mapToInt(i -> (Integer) i).sum();
    }

    @Test
    void dirtyPropagationRecomputesOnlyAffectedNodes() {
        try (IncrementalEngine engine = new IncrementalEngine()) {
            Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
            engine.addSource("a", 1);
            engine.addNode("b", List.of("a"), in -> {
                calls.computeIfAbsent("b", k -> new AtomicInteger()).incrementAndGet();
                return (Integer) in.get(0) + 1;
            });
            engine.addNode("c", List.of("b"), in -> {
                calls.computeIfAbsent("c", k -> new AtomicInteger()).incrementAndGet();
                return (Integer) in.get(0) * 2;
            });
            engine.addSource("x", 10);
            engine.addNode("y", List.of("x"), in -> {
                calls.computeIfAbsent("y", k -> new AtomicInteger()).incrementAndGet();
                return (Integer) in.get(0) + 5;
            });

            assertThat((Integer) engine.get("c")).isEqualTo(4);
            assertThat((Integer) engine.get("y")).isEqualTo(15);
            calls.clear();

            engine.set("a", 100);
            assertThat((Integer) engine.get("c")).isEqualTo(202);

            // b and c recomputed; y untouched by the change on a.
            assertThat(calls.get("b")).hasValue(1);
            assertThat(calls.get("c")).hasValue(1);
            assertThat(calls).doesNotContainKey("y");

            EngineStats stats = engine.stats();
            assertThat(stats.lastRoundRecomputed()).isEqualTo(2);
            assertThat(stats.fullRecomputeCount()).isEqualTo(3);
            assertThat(stats.lastRoundRecomputed()).isLessThan(stats.fullRecomputeCount());
        }
    }

    @Test
    void multipleUpstreamChangesMergeIntoSingleRecompute() {
        try (IncrementalEngine engine = new IncrementalEngine()) {
            AtomicInteger mCalls = new AtomicInteger();
            AtomicInteger nCalls = new AtomicInteger();
            engine.addSource("x", 1);
            engine.addSource("y", 2);
            engine.addNode("m", List.of("x", "y"), in -> {
                mCalls.incrementAndGet();
                return sum(in);
            });
            engine.addNode("n", List.of("m"), in -> {
                nCalls.incrementAndGet();
                return (Integer) in.get(0) * 10;
            });

            assertThat((Integer) engine.get("n")).isEqualTo(30);
            mCalls.set(0);
            nCalls.set(0);

            // Two upstream changes in the same round: downstream recomputes once, not twice.
            engine.set("x", 10);
            engine.set("y", 20);
            assertThat((Integer) engine.get("n")).isEqualTo(300);

            assertThat(mCalls).hasValue(1);
            assertThat(nCalls).hasValue(1);
            assertThat(engine.stats().lastRoundRecomputed()).isEqualTo(2);
        }
    }

    @Test
    void cycleDetectionReportsCyclePath() {
        try (IncrementalEngine engine = new IncrementalEngine()) {
            engine.addNode("a", List.of("c"), IncrementalEngineTest::sumUnchecked);
            engine.addNode("b", List.of("a"), IncrementalEngineTest::sumUnchecked);
            assertThatThrownBy(() -> engine.addNode("c", List.of("b"), IncrementalEngineTest::sumUnchecked))
                    .isInstanceOfSatisfying(CycleDetectedException.class, e ->
                            assertThat(e.cyclePath()).containsExactly("c", "b", "a", "c"))
                    .hasMessageContaining("c -> b -> a -> c");

            // Failed node registration is rolled back: "c" can be registered again,
            // which also makes the previously added nodes a and b computable.
            engine.addSource("c", 1);
            assertThat((Integer) engine.get("b")).isEqualTo(1);
        }
    }

    @Test
    void selfLoopIsDetectedAsCycle() {
        try (IncrementalEngine engine = new IncrementalEngine()) {
            assertThatThrownBy(() -> engine.addNode("a", List.of("a"), IncrementalEngineTest::sumUnchecked))
                    .isInstanceOfSatisfying(CycleDetectedException.class, e ->
                            assertThat(e.cyclePath()).containsExactly("a", "a"));
        }
    }

    private static Object sumUnchecked(List<Object> inputs) {
        return sum(inputs);
    }

    @Test
    void cleanValuesAreServedFromCacheAndCounted() {
        try (IncrementalEngine engine = new IncrementalEngine()) {
            AtomicInteger calls = new AtomicInteger();
            engine.addSource("a", 5);
            engine.addNode("b", List.of("a"), in -> {
                calls.incrementAndGet();
                return (Integer) in.get(0) * 3;
            });

            assertThat((Integer) engine.get("b")).isEqualTo(15);
            assertThat((Integer) engine.get("b")).isEqualTo(15);
            assertThat((Integer) engine.get("b")).isEqualTo(15);

            assertThat(calls).hasValue(1);
            assertThat(engine.stats().cacheHits()).isEqualTo(2);

            // Setting the same value is a no-op: still a cache hit, no recompute.
            engine.set("a", 5);
            assertThat((Integer) engine.get("b")).isEqualTo(15);
            assertThat(calls).hasValue(1);
            assertThat(engine.stats().cacheHits()).isEqualTo(3);
        }
    }

    @Test
    void independentNodesRecomputeInParallelExactlyOnce() {
        int width = 8;
        long sleepMillis = 100;
        try (IncrementalEngine engine = new IncrementalEngine(4)) {
            Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
            engine.addSource("root", 0);
            for (int i = 0; i < width; i++) {
                String name = "n" + i;
                engine.addNode(name, List.of("root"), in -> {
                    calls.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
                    try {
                        Thread.sleep(sleepMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return name;
                });
            }

            engine.set("root", 1);
            long start = System.nanoTime();
            for (int i = 0; i < width; i++) {
                assertThat((String) engine.get("n" + i)).isEqualTo("n" + i);
            }
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

            // Sequential execution would take >= width * sleepMillis = 800ms.
            assertThat(elapsedMillis).isLessThan(width * sleepMillis / 2);
            // Every node computed exactly once despite concurrent scheduling.
            assertThat(calls.values()).allSatisfy(c -> assertThat(c).hasValue(1));
            assertThat(engine.stats().lastRoundRecomputed()).isEqualTo(width);
        }
    }

    @Test
    void statisticsAreReported() {
        try (IncrementalEngine engine = new IncrementalEngine()) {
            engine.addSource("a", 1);
            engine.addNode("b", List.of("a"), in -> (Integer) in.get(0) + 1);
            engine.addNode("c", List.of("b"), in -> (Integer) in.get(0) + 1);

            engine.get("c");
            EngineStats initial = engine.stats();
            assertThat(initial.totalNodes()).isEqualTo(3);
            assertThat(initial.derivedNodes()).isEqualTo(2);
            assertThat(initial.lastRoundRecomputed()).isEqualTo(2);
            assertThat(initial.totalComputeNanos()).isGreaterThan(0);

            engine.set("a", 2);
            engine.get("c");
            engine.get("c");
            EngineStats stats = engine.stats();
            assertThat(stats.lastRoundRecomputed()).isEqualTo(2);
            assertThat(stats.cacheHits()).isEqualTo(1);
            assertThat(stats.totalComputeNanos()).isGreaterThanOrEqualTo(initial.totalComputeNanos());
        }
    }

    @Test
    void unknownAndDuplicateNodesAreRejected() {
        try (IncrementalEngine engine = new IncrementalEngine()) {
            engine.addSource("a", 1);
            assertThatThrownBy(() -> engine.addSource("a", 2))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> engine.get("missing"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> engine.set("missing", 1))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
