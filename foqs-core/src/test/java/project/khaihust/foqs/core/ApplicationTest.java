package project.khaihust.foqs.core;

import config.ShardConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ApplicationTest {

    @Test
    @DisplayName("Should start and stop Application on ephemeral port cleanly")
    void testApplicationLifecycle() throws Exception {
        Application app = new Application(0);
        try {
            app.start();

            assertThat(app.getPort()).isGreaterThan(0);
            assertThat(app.getServer()).isNotNull();
            assertThat(app.getServer().isShutdown()).isFalse();
            assertThat(app.getServer().isTerminated()).isFalse();
            assertThat(app.getProducerBatch()).isNotNull();
            assertThat(app.getDatasourceManager()).isNotNull();
            assertThat(app.getEnqueueService()).isNotNull();
            assertThat(app.getDequeueService()).isNotNull();
            assertThat(app.getPrefetchBufferRegistry()).isNotNull();
            assertThat(app.getLeaseReclaimer()).isNotNull();
        } finally {
            app.stop();
        }

        assertThat(app.getServer().isShutdown()).isTrue();
    }

    @Test
    @DisplayName("Should initialize Application with custom configuration and shard configs")
    void testApplicationCustomConfiguration() throws Exception {
        List<ShardConfig> customShards = List.of(
                ShardConfig.builder()
                        .shardId(0)
                        .jdbcUrl("jdbc:h2:mem:app_custom_shard;DB_CLOSE_DELAY=-1")
                        .username("sa")
                        .password("")
                        .build()
        );

        Application app = new Application(0, 500, 10, 5, customShards);
        try {
            app.start();
            assertThat(app.getPort()).isGreaterThan(0);
            assertThat(app.getServer()).isNotNull();
            assertThat(app.getProducerBatch()).isNotNull();
            assertThat(app.getDatasourceManager()).isNotNull();
            assertThat(app.getEnqueueService()).isNotNull();
            assertThat(app.getDequeueService()).isNotNull();
            assertThat(app.getPrefetchBufferRegistry()).isNotNull();
            assertThat(app.getLeaseReclaimer()).isNotNull();
        } finally {
            app.stop();
        }

        assertThat(app.getServer().isShutdown()).isTrue();
    }

    @Test
    @DisplayName("Should handle multiple stop() calls idempotently without throwing exceptions")
    void testApplicationStopIdempotency() throws Exception {
        Application app = new Application(0);
        app.start();

        assertThatCode(() -> {
            app.stop();
            app.stop();
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should initialize Application using default constructor")
    void testDefaultConstructor() {
        Application app = new Application();
        try {
            assertThat(app.getServer()).isNotNull();
            assertThat(app.getProducerBatch()).isNotNull();
            assertThat(app.getDatasourceManager()).isNotNull();
            assertThat(app.getEnqueueService()).isNotNull();
            assertThat(app.getDequeueService()).isNotNull();
            assertThat(app.getPrefetchBufferRegistry()).isNotNull();
            assertThat(app.getLeaseReclaimer()).isNotNull();
            assertThat(app.getPort()).isEqualTo(8080);
        } finally {
            app.stop();
        }
    }
}
