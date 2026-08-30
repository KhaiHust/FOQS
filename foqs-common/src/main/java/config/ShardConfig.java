package config;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ShardConfig {
     int shardId;
     String jdbcUrl;
     String username;
     String password;
}
