package project.khaihust.foqs.core.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShardRouterTest {

    @Test
    @DisplayName("Should route all topics to the only shard when single shard is configured")
    void testSingleShardRouting() {
        ShardRouter router = new ShardRouter(List.of(0), 128);

        for (int i = 0; i < 100; i++) {
            assertThat(router.selectShard("topic-" + i)).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("Should deterministically route the same topic to the same shard")
    void testDeterministicRouting() {
        ShardRouter router = new ShardRouter(List.of(0, 1, 2), 128);

        for (int i = 0; i < 50; i++) {
            String topic = "deterministic-topic-" + i;
            int firstShard = router.selectShard(topic);
            int secondShard = router.selectShard(topic);
            int thirdShard = router.selectShard(topic);

            assertThat(secondShard).isEqualTo(firstShard);
            assertThat(thirdShard).isEqualTo(firstShard);
            assertThat(firstShard).isIn(0, 1, 2);
        }
    }

    @Test
    @DisplayName("Should distribute topics across all shards reasonably uniformly")
    void testUniformDistributionAcrossShards() {
        List<Integer> shardIds = List.of(0, 1, 2);
        ShardRouter router = new ShardRouter(shardIds, 128);

        Map<Integer, Integer> distribution = new HashMap<>();
        int totalTopics = 1500;

        for (int i = 0; i < totalTopics; i++) {
            int shard = router.selectShard("partitioned-workload-topic-" + i);
            distribution.merge(shard, 1, Integer::sum);
        }

        // Verify all shards were used
        assertThat(distribution.keySet()).containsExactlyInAnyOrder(0, 1, 2);

        // In uniform distribution with 3 shards (expected 500 each), each should receive >= 250
        for (int shard : shardIds) {
            int count = distribution.getOrDefault(shard, 0);
            assertThat(count).isGreaterThanOrEqualTo(250);
        }
    }

    @Test
    @DisplayName("Should reject null or empty shard IDs")
    void testInvalidShardIds() {
        assertThatThrownBy(() -> new ShardRouter(null, 128))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shardIds must not be null or empty");

        assertThatThrownBy(() -> new ShardRouter(List.of(), 128))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shardIds must not be null or empty");
    }

    @Test
    @DisplayName("Should reject non-positive virtual nodes count")
    void testInvalidVirtualNodes() {
        assertThatThrownBy(() -> new ShardRouter(List.of(0), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("virtualNodes must be greater than 0");

        assertThatThrownBy(() -> new ShardRouter(List.of(0), -5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("virtualNodes must be greater than 0");
    }

    @Test
    @DisplayName("Should reject null or blank topic in selectShard")
    void testInvalidTopic() {
        ShardRouter router = new ShardRouter(List.of(0, 1), 64);

        assertThatThrownBy(() -> router.selectShard(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Topic must not be null or blank");

        assertThatThrownBy(() -> router.selectShard(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Topic must not be null or blank");

        assertThatThrownBy(() -> router.selectShard("   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Topic must not be null or blank");
    }
}
