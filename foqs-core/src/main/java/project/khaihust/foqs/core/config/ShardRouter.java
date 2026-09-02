package project.khaihust.foqs.core.config;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ShardRouter {
    private final NavigableMap<Long, Integer> ring = new TreeMap<>();
    private final int virtualNodes;

    public ShardRouter(List<Integer> shardIds, int virtualNodes) {
        if (shardIds == null || shardIds.isEmpty()) {
            throw new IllegalArgumentException("shardIds must not be null or empty");
        }
        if (virtualNodes <= 0) {
            throw new IllegalArgumentException("virtualNodes must be greater than 0");
        }
        this.virtualNodes = virtualNodes;
        for (int shardId : shardIds) {
            for (int i = 0; i < virtualNodes; i++) {
                var key = buildKey(shardId, i);
                var hash = hash(key);
                ring.put(hash, shardId);
            }
        }
    }

    public int selectShard(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic must not be null or blank");
        }
        var hash = hash(topic);
        var entry = ring.ceilingEntry(hash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    public int getVirtualNodes() {
        return virtualNodes;
    }

    private long hash(String key) {
        var h = 0xcbf29ce484222325L;
        for (byte b : key.getBytes(StandardCharsets.UTF_8)) {
            h ^= (b & 0xFF);
            h *= 0x100000001b3L;
        }
        return h;
    }

    private String buildKey(int shardId, int virtualId) {
        return "shard-" + shardId + "-vnode-" + virtualId;
    }
}

