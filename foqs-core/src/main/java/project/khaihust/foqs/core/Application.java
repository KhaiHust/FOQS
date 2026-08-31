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
import project.khaihust.foqs.core.buffer.impl.ProducerBatch;
import project.khaihust.foqs.core.config.DatasourceManager;
import project.khaihust.foqs.core.service.EnqueueService;
import project.khaihust.foqs.core.storage.SingleShardQueueRepository;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    private static final Integer DEFAULT_PORT = 8080;
    private static final Integer DEFAULT_BUFFER_CAPACITY = 10_000;
    private static final Integer DEFAULT_BATCH_THRESHOLD = 100;
    private static final Integer DEFAULT_FLUSH_INTERVAL_MS = 10;

    @Getter
    private final Server server;
    @Getter
    private final ProducerBatch producerBatch;
    @Getter
    private final DatasourceManager datasourceManager;
    @Getter
    private final EnqueueService enqueueService;
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
                AppConfig.getShardConfigs()
        );
    }

    public Application(int port, int bufferCapacity, int batchThreshold, int flushIntervalMs, List<ShardConfig> shardConfigs) {
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

        this.enqueueService = new EnqueueService(this.producerBatch);

        this.server = ServerBuilder.forPort(port)
                .addService(this.enqueueService)
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
