package com.example.incremental;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class IncrementalEngineTest {

    private final IncrementalEngine engine = new IncrementalEngine(4);

    @AfterEach
    void tearDown() {
        engine.close();
    }

    @Test
    void dirtyPropagationRecomputesOnlyAffectedNodes() {
        AtomicInteger bCalls = new AtomicInteger();
        AtomicInteger cCalls = new AtomicInteger();
        AtomicInteger dCalls = new AtomicInteger();
        AtomicInteger sideCalls = new AtomicInteger();

        engine.addNode(Node.source("a"));
        engine.addNode(Node.derived("b", List.of("a"), deps -> {
            bCalls.incrementAndGet();
            return deps.<Integer>get("a") * 2;
        }));
        engine.addNode(Node.derived("c", List.of("b"), deps -> {
            cCalls.incrementAndGet();
            return deps.<Integer>get("b") + 1;
        }));
        engine.addNode(Node.derived("d", List.of("c"), deps -> {
            dCalls.incrementAndGet();
            return deps.<Integer>get("c") + 1;
        }));
        engine.addNode(Node.derived("side", List.of(), deps -> {
            sideCalls.incrementAndGet();
            return 99;
        }));

        engine.setSource("a", 1);
        RecomputeReport initial = engine.recompute();
        assertThat(initial.recomputedCount()).isEqualTo(4);
        assertThat(initial.fullRecomputeNodes()).isEqualTo(4);
        assertThat(engine.<Integer>get("d")).isEqualTo(4);

        engine.setSource("a", 10);
        RecomputeReport report = engine.recompute();

        assertThat(report.recomputedNodeIds()).containsExactly("b", "c", "d");
        assertThat(report.recomputedCount()).isEqualTo(3);
        assertThat(report.fullRecomputeNodes()).isEqualTo(4);
        assertThat(report.skippedCount()).isEqualTo(1);
        assertThat(sideCalls.get()).isEqualTo(1);
        assertThat(bCalls.get()).isEqualTo(2);
        assertThat(cCalls.get()).isEqualTo(2);
        assertThat(dCalls.get()).isEqualTo(2);
        assertThat(engine.<Integer>get("d")).isEqualTo(22);
    }

    @Test
    void mergedRecomputeComputesDownstreamOnlyOncePerRound() {
        AtomicInteger joinCalls = new AtomicInteger();

        engine.addNode(Node.source("s1"));
        engine.addNode(Node.derived("m1", List.of("s1"), deps -> deps.<Integer>get("s1") * 10));
        engine.addNode(Node.source("s2"));
        engine.addNode(Node.derived("m2", List.of("s2"), deps -> deps.<Integer>get("s2") * 100));
        engine.addNode(Node.derived("join", List.of("m1", "m2"), deps -> {
            joinCalls.incrementAndGet();
            return deps.<Integer>get("m1") + deps.<Integer>get("m2");
        }));

        engine.setSource("s1", 1);
        engine.setSource("s2", 2);
        RecomputeReport initial = engine.recompute();
        assertThat(initial.recomputedCount()).isEqualTo(3);
        assertThat(joinCalls.get()).isEqualTo(1);

        engine.setSource("s1", 3);
        engine.setSource("s2", 4);
        RecomputeReport merged = engine.recompute();

        assertThat(merged.recomputedNodeIds()).containsExactlyInAnyOrder("m1", "m2", "join");
        assertThat(merged.recomputedCount()).isEqualTo(3);
        assertThat(joinCalls.get()).isEqualTo(2);
        assertThat(engine.<Integer>get("join")).isEqualTo(430);
    }

    @Test
    void cycleDetectionReportsCyclePath() {
        engine.addNode(Node.derived("n1", List.of("n3"), deps -> deps.<Integer>get("n3")));
        engine.addNode(Node.derived("n2", List.of("n1"), deps -> deps.<Integer>get("n1") + 1));

        assertThatThrownBy(() -> engine.addNode(
                Node.derived("n3", List.of("n2"), deps -> deps.<Integer>get("n2") + 1)))
                .isInstanceOfSatisfying(CycleDetectedException.class, e -> {
                    assertThat(e.cyclePath()).containsExactly("n3", "n2", "n1", "n3");
                    assertThat(e.getMessage()).contains("n3 -> n2 -> n1 -> n3");
                });

        assertThatThrownBy(() -> engine.addNode(
                Node.derived("self", List.of("self"), deps -> 0)))
                .isInstanceOfSatisfying(CycleDetectedException.class, e ->
                        assertThat(e.cyclePath()).containsExactly("self", "self"));

        assertThatThrownBy(() -> engine.recompute())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("不存在的节点");
    }

    @Test
    void cacheHitAvoidsRecomputation() {
        AtomicInteger calls = new AtomicInteger();
        engine.addNode(Node.source("a"));
        engine.addNode(Node.derived("b", List.of("a"), deps -> {
            calls.incrementAndGet();
            return deps.<Integer>get("a") + 1;
        }));
        engine.setSource("a", 1);

        assertThat(engine.<Integer>get("b")).isEqualTo(2);
        assertThat(calls.get()).isEqualTo(1);
        assertThat(engine.stats().cacheHits()).isZero();

        assertThat(engine.<Integer>get("b")).isEqualTo(2);
        assertThat(engine.<Integer>get("b")).isEqualTo(2);
        assertThat(calls.get()).isEqualTo(1);
        assertThat(engine.stats().cacheHits()).isEqualTo(2);

        engine.setSource("a", 5);
        assertThat(engine.<Integer>get("b")).isEqualTo(6);
        assertThat(calls.get()).isEqualTo(2);
        assertThat(engine.stats().cacheHits()).isEqualTo(2);
    }

    @Test
    void independentNodesRecomputeInParallelAndExactlyOnce() {
        CountDownLatch bothStarted = new CountDownLatch(2);
        AtomicInteger leftCalls = new AtomicInteger();
        AtomicInteger rightCalls = new AtomicInteger();

        engine.addNode(Node.source("in"));
        engine.addNode(Node.derived("left", List.of("in"), deps -> {
            leftCalls.incrementAndGet();
            await(bothStarted);
            return deps.<Integer>get("in") + 1;
        }));
        engine.addNode(Node.derived("right", List.of("in"), deps -> {
            rightCalls.incrementAndGet();
            await(bothStarted);
            return deps.<Integer>get("in") + 2;
        }));
        engine.addNode(Node.derived("sum", List.of("left", "right"),
                deps -> deps.<Integer>get("left") + deps.<Integer>get("right")));

        engine.setSource("in", 1);
        RecomputeReport first = engine.recompute();
        assertThat(first.recomputedCount()).isEqualTo(3);
        assertThat(engine.<Integer>get("sum")).isEqualTo(5);

        engine.setSource("in", 10);
        RecomputeReport second = engine.recompute();

        assertThat(second.recomputedNodeIds()).containsExactlyInAnyOrder("left", "right", "sum");
        assertThat(leftCalls.get()).isEqualTo(2);
        assertThat(rightCalls.get()).isEqualTo(2);
        assertThat(engine.<Integer>get("sum")).isEqualTo(23);
    }

    @Test
    void statsExposeNodeCountRecomputeCountCacheHitsAndComputeTime() {
        engine.addNode(Node.source("a"));
        engine.addNode(Node.derived("b", List.of("a"), deps -> {
            sleepQuietly(5);
            return deps.<Integer>get("a") * 2;
        }));
        engine.addNode(Node.derived("c", List.of("b"), deps -> deps.<Integer>get("b") * 2));

        engine.setSource("a", 1);
        engine.recompute();
        engine.get("c");
        engine.get("c");

        EngineStats stats = engine.stats();
        assertThat(stats.totalNodes()).isEqualTo(3);
        assertThat(stats.lastRecomputedNodes()).isEqualTo(2);
        assertThat(stats.fullRecomputeNodes()).isEqualTo(2);
        assertThat(stats.cacheHits()).isEqualTo(2);
        assertThat(stats.totalComputeNanos()).isGreaterThan(0L);
        assertThat(stats.totalComputeMillis()).isGreaterThan(0.0);

        engine.setSource("a", 2);
        engine.recompute();
        EngineStats afterSecond = engine.stats();
        assertThat(afterSecond.lastRecomputedNodes()).isEqualTo(2);
        assertThat(afterSecond.totalComputeNanos()).isGreaterThan(stats.totalComputeNanos());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.countDown();
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("两个分支未并行执行（等待超时）");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
