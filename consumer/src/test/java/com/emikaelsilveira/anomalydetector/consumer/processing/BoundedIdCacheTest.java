package com.emikaelsilveira.anomalydetector.consumer.processing;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BoundedIdCacheTest {

    @Test
    void firstSeenIdsBecomeDuplicatesOnlyAfterTheyAreRemembered() {
        BoundedIdCache cache = new BoundedIdCache(2);
        UUID id = UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93");

        assertThat(cache.contains(id)).isFalse();
        cache.remember(id);
        assertThat(cache.contains(id)).isTrue();
    }

    @Test
    void evictsTheOldestIdFirst() {
        BoundedIdCache cache = new BoundedIdCache(2);
        UUID first = UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93");
        UUID second = UUID.fromString("15a5d6a9-14a5-4a4d-9dbb-49ce53e5e9ae");
        UUID third = UUID.fromString("dbcf231c-839f-4fc1-94c4-929e46f735f0");

        cache.remember(first);
        cache.remember(second);
        cache.remember(third);

        assertThat(cache.contains(first)).isFalse();
        assertThat(cache.contains(second)).isTrue();
        assertThat(cache.contains(third)).isTrue();
    }

    @Test
    void remainsBoundedAtItsConfiguredCapacity() {
        BoundedIdCache cache = new BoundedIdCache(3);
        UUID first = UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93");
        UUID second = UUID.fromString("15a5d6a9-14a5-4a4d-9dbb-49ce53e5e9ae");
        UUID third = UUID.fromString("dbcf231c-839f-4fc1-94c4-929e46f735f0");
        UUID fourth = UUID.fromString("801155bf-4d32-4fa9-9ddf-6849781794bd");

        cache.remember(first);
        cache.remember(second);
        cache.remember(third);
        cache.remember(fourth);

        assertThat(cache.contains(first)).isFalse();
        assertThat(cache.contains(second)).isTrue();
        assertThat(cache.contains(third)).isTrue();
        assertThat(cache.contains(fourth)).isTrue();
    }
}
