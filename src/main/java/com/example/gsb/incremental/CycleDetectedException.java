package com.example.gsb.incremental;

import java.util.List;

/** Thrown when adding a node would introduce a dependency cycle. */
public class CycleDetectedException extends RuntimeException {

    private final List<String> cyclePath;

    public CycleDetectedException(List<String> cyclePath) {
        super("Dependency cycle detected: " + String.join(" -> ", cyclePath));
        this.cyclePath = List.copyOf(cyclePath);
    }

    /** The cycle as a path of node names, e.g. [a, b, c, a]. */
    public List<String> cyclePath() {
        return cyclePath;
    }
}
