package com.emikaelsilveira.anomalydetector.consumer.processing;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Retains a bounded FIFO set of recently processed IDs for in-memory delivery deduplication.
 * Access is serialized by the single listener container.
 */
public final class BoundedIdCache {

    private final int capacity;
    private final Set<UUID> ids = new HashSet<>();
    private final ArrayDeque<UUID> insertionOrder = new ArrayDeque<>();

    public BoundedIdCache(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
    }

    public boolean contains(UUID id) {
        return ids.contains(id);
    }

    public void remember(UUID id) {
        UUID requiredId = Objects.requireNonNull(id, "id");
        if (!ids.add(requiredId)) {
            return;
        }
        insertionOrder.addLast(requiredId);
        if (insertionOrder.size() > capacity) {
            ids.remove(insertionOrder.removeFirst());
        }
    }
}
