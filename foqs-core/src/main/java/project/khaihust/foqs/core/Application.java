package project.khaihust.foqs.core;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import config.AppConfig;
import config.ShardConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project.khaihust.foqs.core.buffer.impl.LeaseReclaimer;
import project.khaihust.foqs.core.buffer.impl.PrefetchBufferRegistry;
import project.khaihust.foqs.core.buffer.impl.ProducerBatch;
import project.khaihust.foqs.core.config.DatasourceManager;
import project.khaihust.foqs.core.service.DequeueService;
import project.khaihust.foqs.core.service.EnqueueService;
import project.khaihust.foqs.core.storage.SingleShardQueueRepository;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    private static final Integer DEFAULT_PORT = 8080;
    private static final Integer DEFAULT_BUFFER_CAPACITY = 10_000;
    private static final Integer DEFAULT_BATCH_THRESHOLD = 100;
    private static final Integer DEFAULT_FLUSH_INTERVAL_MS = 10;
    private static final Integer DEFAULT_PREFETCH_TARGET_CAPACITY = 500;
    private static final Integer DEFAULT_LEASE_DURATION_SECONDS = 30;
    private static final Integer DEFAULT_REFILL_INTERVAL_MS = 1000;
    private static final Integer DEFAULT_RECLAIMER_INTERVAL_SECONDS = 5;

    @Getter
    private final Server server;
    @Getter
    private final ProducerBatch producerBatch;
    @Getter
    private final PrefetchBufferRegistry prefetchBufferRegistry;
    @Getter
    private final LeaseReclaimer leaseReclaimer;
    @Getter
    private final DatasourceManager datasourceManager;
    @Getter
    private final EnqueueService enqueueService;
    @Getter
    private final DequeueService dequeueService;

    private final int configuredPort;

    public Application() {
        this(AppConfig.getIntProperty("foqs.server.port", DEFAULT_PORT));
    }

    public Application(int port) {
        this(
                port,
                AppConfig.getIntProperty("foqs.buffer.capacity", DEFAULT_BUFFER_CAPACITY),
                AppConfig.getIntProperty("foqs.buffer.batch-size-threshold", DEFAULT_BATCH_THRESHOLD),
                AppConfig.getIntProperty("foqs.buffer.flush-interval-ms", DEFAULT_FLUSH_INTERVAL_MS),
                AppConfig.getIntProperty("foqs.prefetch.target-capacity", DEFAULT_PREFETCH_TARGET_CAPACITY),
                Duration.ofSeconds(AppConfig.getIntProperty("foqs.prefetch.lease-duration-seconds", DEFAULT_LEASE_DURATION_SECONDS)),
                AppConfig.getIntProperty("foqs.prefetch.refill-interval-ms", DEFAULT_REFILL_INTERVAL_MS),
                AppConfig.getIntProperty("foqs.reclaimer.interval-seconds", DEFAULT_RECLAIMER_INTERVAL_SECONDS),
                AppConfig.getShardConfigs()
        );
    }

    public Application(int port, int bufferCapacity, int batchThreshold, int flushIntervalMs, List<ShardConfig> shardConfigs) {
        this(
                port,
                bufferCapacity,
                batchThreshold,
                flushIntervalMs,
                DEFAULT_PREFETCH_TARGET_CAPACITY,
                Duration.ofSeconds(DEFAULT_LEASE_DURATION_SECONDS),
                DEFAULT_REFILL_INTERVAL_MS,
                DEFAULT_RECLAIMER_INTERVAL_SECONDS,
                shardConfigs
        );
    }

    public Application(int port,
                       int bufferCapacity,
                       int batchThreshold,
                       int flushIntervalMs,
                       int prefetchTargetCapacity,
                       Duration prefetchLeaseDuration,
                       long prefetchRefillIntervalMs,
                       int reclaimerIntervalSeconds,
                       List<ShardConfig> shardConfigs) {
        this.configuredPort = port;
        this.datasourceManager = new DatasourceManager(shardConfigs);
        var shardDatasource = this.datasourceManager.getDataSource(0);
        var singleShardQueueRepository = new SingleShardQueueRepository(shardDatasource);

        this.producerBatch = new ProducerBatch(
                singleShardQueueRepository,
                bufferCapacity,
                batchThreshold,
                flushIntervalMs
        );

        this.prefetchBufferRegistry = new PrefetchBufferRegistry(
                singleShardQueueRepository,
                prefetchTargetCapacity,
                prefetchLeaseDuration,
                prefetchRefillIntervalMs
        );

        this.leaseReclaimer = new LeaseReclaimer(
                singleShardQueueRepository,
                reclaimerIntervalSeconds
        );

        this.enqueueService = new EnqueueService(this.producerBatch);
        this.dequeueService = new DequeueService(this.prefetchBufferRegistry, singleShardQueueRepository);

        this.server = ServerBuilder.forPort(port)
                .addService(this.enqueueService)
                .addService(this.dequeueService)
                .build();
    }

    public void start() throws IOException {
        server.start();
        logger.info("FOQS gRPC Server started, listening on port {}", getPort());
    }

    public void stop() {
        logger.info("Shutting down gRPC Server");
        try {
            if (server != null) {
                server.shutdown();
                if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            }
            if (producerBatch != null) {
                producerBatch.close();
            }
            if (prefetchBufferRegistry != null) {
                prefetchBufferRegistry.close();
            }
            if (leaseReclaimer != null) {
                leaseReclaimer.close();
            }
            if (datasourceManager != null) {
                datasourceManager.close();
            }
            logger.info("FOQS Server terminated successfully.");
        } catch (Exception e) {
            logger.error("Error while shutting down gRPC Server.", e);
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public int getPort() {
        try {
            if (server != null && server.getPort() > 0) {
                return server.getPort();
            }
        } catch (IllegalStateException e) {
            // Server not started yet
        }
        return configuredPort;
    }

    public static void main(String[] args) throws Exception {
        LoggerContext ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
        ctx.getLogger(Logger.ROOT_LOGGER_NAME).setLevel(Level.INFO);
        logger.info("Application started");

        var app = new Application();
        app.start();

        Runtime.getRuntime().addShutdownHook(new Thread(app::stop));
        app.blockUntilShutdown();
    }
}
