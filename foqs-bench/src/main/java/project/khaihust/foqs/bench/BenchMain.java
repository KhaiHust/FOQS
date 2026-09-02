package project.khaihust.foqs.bench;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;

/**
 * CLI entry point for foqs-bench.
 *
 * <p>Orchestrates open-loop load generation, consumer workers, EXPLAIN
 * capture, InnoDB monitoring, and CSV result output.
 *
 * <pre>
 * java -Xms2g -Xmx2g -jar foqs-bench.jar \
 *   --experiment baseline \
 *   --target-rate 10000 \
 *   [options...]
 * </pre>
 */
public class BenchMain {
    private static final Logger logger = LoggerFactory.getLogger(BenchMain.class);

    // ── Defaults ──
    private static final String DEF_HOST = "localhost";
    private static final int DEF_PORT = 8080;
    private static final int DEF_WARMUP = 60;
    private static final int DEF_DURATION = 180;
    private static final int DEF_PAYLOAD_BYTES = 256;
    private static final int DEF_CHANNELS = 8;
    private static final int DEF_MAX_INFLIGHT = 2048;
    private static final String DEF_TOPIC = "bench-topic";
    private static final int DEF_TOPICS = 60;
    private static final String DEF_OUTPUT = "bench-results.csv";
    private static final int DEF_CONSUMERS = 4;
    private static final int DEF_CONSUMER_BATCH = 100;
    private static final int DEF_SHARD_COUNT = 1;
    private static final String DEF_BUFFER_POOL = "4G";
    private static final int DEF_BATCH_THRESHOLD = 100;
    private static final int DEF_FLUSH_INTERVAL = 10;
    private static final int DEF_MAX_POOL_SIZE = 20;
    private static final String DEF_JVM_FLAGS = "";
    private static final int DEF_REPEAT_INDEX = 0;
    private static final int DEF_CONSUMER_RATE = 0;
    private static final String DEF_MYSQL_URL = "jdbc:mysql://localhost:3306/foqs_shard_0?useSSL=false&allowPublicKeyRetrieval=true";
    private static final String DEF_MYSQL_USER = "root";
    private static final String DEF_MYSQL_PASS = "root";

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parseArgs(args);

        String experiment = requireOpt(opts, "experiment");
        int targetRate = Integer.parseInt(requireOpt(opts, "target-rate"));

        // Connection options
        String host = opt(opts, "host", DEF_HOST);
        int port = Integer.parseInt(opt(opts, "port", String.valueOf(DEF_PORT)));
        int warmup = Integer.parseInt(opt(opts, "warmup", String.valueOf(DEF_WARMUP)));
        int duration = Integer.parseInt(opt(opts, "duration", String.valueOf(DEF_DURATION)));
        int payloadBytes = Integer.parseInt(opt(opts, "payload-bytes", String.valueOf(DEF_PAYLOAD_BYTES)));
        int channels = Integer.parseInt(opt(opts, "channels", String.valueOf(DEF_CHANNELS)));
        int maxInflight = Integer.parseInt(opt(opts, "max-inflight", String.valueOf(DEF_MAX_INFLIGHT)));
        String topic = opt(opts, "topic", DEF_TOPIC);
        int topicCount = Integer.parseInt(opt(opts, "topics", String.valueOf(DEF_TOPICS)));
        String output = opt(opts, "output", DEF_OUTPUT);
        int consumers = Integer.parseInt(opt(opts, "consumers", String.valueOf(DEF_CONSUMERS)));
        int consumerBatch = Integer.parseInt(opt(opts, "consumer-batch", String.valueOf(DEF_CONSUMER_BATCH)));
        int consumerRate = Integer.parseInt(opt(opts, "consumer-rate", String.valueOf(DEF_CONSUMER_RATE)));

        // Topics list
        List<String> topics;
        if (topicCount <= 1) {
            topics = List.of(topic);
        } else {
            topics = new ArrayList<>();
            for (int i = 0; i < topicCount; i++) {
                topics.add(topic + "-" + i);
            }
        }

        // Metadata (recorded in CSV, do not affect bench behavior)
        int shardCount = Integer.parseInt(opt(opts, "shard-count", String.valueOf(DEF_SHARD_COUNT)));
        String bufferPoolSize = opt(opts, "buffer-pool-size", DEF_BUFFER_POOL);
        int batchThreshold = Integer.parseInt(opt(opts, "batch-threshold", String.valueOf(DEF_BATCH_THRESHOLD)));
        int flushIntervalMs = Integer.parseInt(opt(opts, "flush-interval-ms", String.valueOf(DEF_FLUSH_INTERVAL)));
        int maxPoolSize = Integer.parseInt(opt(opts, "max-pool-size", String.valueOf(DEF_MAX_POOL_SIZE)));
        String jvmFlags = opt(opts, "jvm-flags", DEF_JVM_FLAGS);
        int repeatIndex = Integer.parseInt(opt(opts, "repeat-index", String.valueOf(DEF_REPEAT_INDEX)));

        // MySQL connection (for EXPLAIN / InnoDB monitoring / TRUNCATE)
        String mysqlUrl = opt(opts, "mysql-url", DEF_MYSQL_URL);
        String mysqlUser = opt(opts, "mysql-user", DEF_MYSQL_USER);
        String mysqlPass = opt(opts, "mysql-password", DEF_MYSQL_PASS);

        List<String> mysqlUrls = new ArrayList<>();
        String mysqlUrlsOpt = opt(opts, "mysql-urls", null);
        if (mysqlUrlsOpt != null && !mysqlUrlsOpt.isBlank()) {
            mysqlUrls.addAll(Arrays.stream(mysqlUrlsOpt.split(",")).map(String::trim).toList());
        } else if (shardCount > 1) {
            for (int i = 0; i < shardCount; i++) {
                mysqlUrls.add("jdbc:mysql://localhost:" + (3306 + i) + "/foqs_shard_" + i + "?useSSL=false&allowPublicKeyRetrieval=true");
            }
        } else {
            mysqlUrls.add(mysqlUrl);
        }

        // Resolve git SHA
        String gitSha = resolveGitSha();

        logger.info("╔══════════════════════════════════════════╗");
        logger.info("║          FOQS BENCHMARK - {}",
                String.format("%-14s║", experiment));
        logger.info("╠══════════════════════════════════════════╣");
        logger.info("║ Target rate  : {} msg/s", targetRate);
        logger.info("║ Warmup       : {}s  Measure: {}s", warmup, duration);
        logger.info("║ Channels     : {}   Inflight: {}", channels, maxInflight);
        logger.info("║ Consumers    : {}   Payload : {}B", consumers, payloadBytes);
        logger.info("║ Batch thresh : {}   Flush   : {}ms", batchThreshold, flushIntervalMs);
        logger.info("║ Repeat       : {}   Git     : {}", repeatIndex, gitSha);
        logger.info("║ Topics       : {} (count={})", topic, topicCount);
        logger.info("║ Shards       : count={} urls={}", shardCount, mysqlUrls);
        logger.info("╚══════════════════════════════════════════╝");

        switch (experiment) {
            case "baseline", "batch_sweep", "smoke" ->
                    runStandardExperiment(
                            host, port, warmup, duration, payloadBytes, channels, maxInflight,
                            topics, topicCount, output, consumers, consumerBatch, consumerRate,
                            shardCount, bufferPoolSize, batchThreshold, flushIntervalMs,
                            maxPoolSize, jvmFlags, repeatIndex, targetRate, experiment,
                            gitSha, mysqlUrls, mysqlUser, mysqlPass
                    );
            case "backlog" ->
                    runBacklogExperiment(
                            host, port, warmup, duration, payloadBytes, channels, maxInflight,
                            topics, topicCount, output, consumers, consumerBatch,
                            shardCount, bufferPoolSize, batchThreshold, flushIntervalMs,
                            maxPoolSize, jvmFlags, repeatIndex, targetRate, experiment,
                            gitSha, mysqlUrls, mysqlUser, mysqlPass
                    );
            case "lease_recovery" ->
                    runLeaseRecoveryExperiment(
                            host, port, payloadBytes, channels, maxInflight,
                            topics, topicCount, output, consumers, consumerBatch,
                            shardCount, bufferPoolSize, batchThreshold, flushIntervalMs,
                            maxPoolSize, jvmFlags, repeatIndex, targetRate,
                            gitSha, mysqlUrls, mysqlUser, mysqlPass
                    );
            default -> {
                logger.error("Unknown experiment: {}. Valid: baseline, batch_sweep, backlog, lease_recovery, smoke", experiment);
                System.exit(1);
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Standard experiment (baseline / batch_sweep / smoke)
    // ═══════════════════════════════════════════════════════════════
    private static void runStandardExperiment(
            String host, int port, int warmup, int duration, int payloadBytes,
            int channels, int maxInflight, List<String> topics, int topicCount, String output,
            int numConsumers, int consumerBatch, int consumerRate,
            int shardCount, String bufferPoolSize, int batchThreshold, int flushIntervalMs,
            int maxPoolSize, String jvmFlags, int repeatIndex, int targetRate,
            String experiment, String gitSha,
            List<String> mysqlUrls, String mysqlUser, String mysqlPass
    ) throws Exception {

        // Purge queue before run across all shards
        truncateAllQueues(mysqlUrls, mysqlUser, mysqlPass);

        ExplainCapture explain = new ExplainCapture(mysqlUrls.get(0), mysqlUser, mysqlPass);

        try (CpuSampler cpuSampler = new CpuSampler();
             ChannelPool pool = new ChannelPool(host, port, channels);
             CsvResultWriter csv = new CsvResultWriter(Path.of(output))) {

            cpuSampler.start();

            // Start consumers at full speed (or specified rate) to drain the queue
            List<ConsumerWorker> consumers = new ArrayList<>();
            List<Thread> consumerThreads = new ArrayList<>();
            for (int i = 0; i < numConsumers; i++) {
                int offset = i * (topics.size() / Math.max(1, numConsumers));
                var worker = new ConsumerWorker(pool, topics, consumerBatch, 1000, true, consumerRate, offset);
                var thread = new Thread(worker, "consumer-" + i);
                thread.setDaemon(true);
                thread.start();
                consumers.add(worker);
                consumerThreads.add(thread);
            }

            // Run open-loop generator
            var generator = new OpenLoopLoadGenerator(
                    pool, targetRate, warmup, duration, payloadBytes, maxInflight, topics);
            BenchmarkResult result = generator.run();

            // Compute dequeue throughput
            long totalDequeued = consumers.stream().mapToLong(ConsumerWorker::getTotalDequeued).sum();
            double dequeueTps = (totalDequeued * 1000.0) / result.measurementDurationMs();

            // Capture EXPLAIN
            String explainResult = explain.capture();

            // Stop consumers
            consumers.forEach(ConsumerWorker::stop);
            consumerThreads.forEach(t -> t.interrupt());
            for (Thread t : consumerThreads) t.join(5000);

            // Shard row counts and skew
            List<Long> rowsPerShard = countRowsPerShard(mysqlUrls, mysqlUser, mysqlPass);
            long totalRows = rowsPerShard.stream().mapToLong(Long::longValue).sum();
            double observedShardSkew = 0.0;
            if (shardCount > 1 && totalRows > 0) {
                double even = (double) totalRows / shardCount;
                double maxDev = 0.0;
                for (long c : rowsPerShard) {
                    maxDev = Math.max(maxDev, Math.abs(c - even) / even);
                }
                observedShardSkew = maxDev;
            }
            String messagesPerShard = rowsPerShard.toString();
            double hostCpuPct = cpuSampler.getAverageCpuPct();

            // Build result with dequeue throughput, explain, topics, skew, cpu, shard counts
            BenchmarkResult finalResult = new BenchmarkResult(
                    result.histogram(), result.successCount(), result.errorCount(),
                    result.measurementDurationMs(), dequeueTps, explainResult,
                    topicCount, observedShardSkew, hostCpuPct, messagesPerShard
            );

            csv.writeRow(gitSha, shardCount, bufferPoolSize, batchThreshold, flushIntervalMs,
                    maxPoolSize, jvmFlags, targetRate, payloadBytes, experiment, repeatIndex, finalResult);

            printSummary(finalResult, targetRate);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Backlog experiment (enqueue >> dequeue, watch p99 degrade)
    // ═══════════════════════════════════════════════════════════════
    private static void runBacklogExperiment(
            String host, int port, int warmup, int duration, int payloadBytes,
            int channels, int maxInflight, List<String> topics, int topicCount, String output,
            int numConsumers, int consumerBatch,
            int shardCount, String bufferPoolSize, int batchThreshold, int flushIntervalMs,
            int maxPoolSize, String jvmFlags, int repeatIndex, int targetRate,
            String experiment, String gitSha,
            List<String> mysqlUrls, String mysqlUser, String mysqlPass
    ) throws Exception {

        truncateAllQueues(mysqlUrls, mysqlUser, mysqlPass);

        ExplainCapture explain = new ExplainCapture(mysqlUrls.get(0), mysqlUser, mysqlPass);

        // Start InnoDB monitor for history list length tracking
        InnodbMonitor monitor = new InnodbMonitor(mysqlUrls.get(0), mysqlUser, mysqlPass);
        Path innodbCsv = Path.of("innodb_history_" + System.currentTimeMillis() + ".csv");
        monitor.start(innodbCsv, 10);

        try (CpuSampler cpuSampler = new CpuSampler();
             ChannelPool pool = new ChannelPool(host, port, channels);
             CsvResultWriter csv = new CsvResultWriter(Path.of(output))) {

            cpuSampler.start();

            // Consumers throttled to 50% of enqueue rate (= targetRate/2 total across all consumers)
            int consumerRatePerWorker = Math.max(1, targetRate / (2 * numConsumers));
            List<ConsumerWorker> consumers = new ArrayList<>();
            List<Thread> consumerThreads = new ArrayList<>();
            for (int i = 0; i < numConsumers; i++) {
                int offset = i * (topics.size() / Math.max(1, numConsumers));
                var worker = new ConsumerWorker(pool, topics, consumerBatch, 1000, true, consumerRatePerWorker, offset);
                var thread = new Thread(worker, "consumer-backlog-" + i);
                thread.setDaemon(true);
                thread.start();
                consumers.add(worker);
                consumerThreads.add(thread);
            }

            // Run open-loop generator
            var generator = new OpenLoopLoadGenerator(
                    pool, targetRate, warmup, duration, payloadBytes, maxInflight, topics);
            BenchmarkResult result = generator.run();

            // Capture EXPLAIN at end (when backlog is large — may show filesort)
            String explainBefore = explain.capture();
            logger.info("EXPLAIN at end of backlog run: {}", explainBefore);

            // Dequeue throughput
            long totalDequeued = consumers.stream().mapToLong(ConsumerWorker::getTotalDequeued).sum();
            double dequeueTps = (totalDequeued * 1000.0) / result.measurementDurationMs();

            // InnoDB history list length
            long hll = monitor.snapshotHistoryListLength();
            logger.info("InnoDB History List Length at end of backlog: {}", hll);

            consumers.forEach(ConsumerWorker::stop);
            consumerThreads.forEach(t -> t.interrupt());
            for (Thread t : consumerThreads) t.join(5000);

            List<Long> rowsPerShard = countRowsPerShard(mysqlUrls, mysqlUser, mysqlPass);
            long totalRows = rowsPerShard.stream().mapToLong(Long::longValue).sum();
            double observedShardSkew = 0.0;
            if (shardCount > 1 && totalRows > 0) {
                double even = (double) totalRows / shardCount;
                double maxDev = 0.0;
                for (long c : rowsPerShard) {
                    maxDev = Math.max(maxDev, Math.abs(c - even) / even);
                }
                observedShardSkew = maxDev;
            }
            String messagesPerShard = rowsPerShard.toString();
            double hostCpuPct = cpuSampler.getAverageCpuPct();

            BenchmarkResult finalResult = new BenchmarkResult(
                    result.histogram(), result.successCount(), result.errorCount(),
                    result.measurementDurationMs(), dequeueTps, explainBefore,
                    topicCount, observedShardSkew, hostCpuPct, messagesPerShard
            );

            csv.writeRow(gitSha, shardCount, bufferPoolSize, batchThreshold, flushIntervalMs,
                    maxPoolSize, jvmFlags, targetRate, payloadBytes, experiment, repeatIndex, finalResult);

            printSummary(finalResult, targetRate);
        } finally {
            monitor.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Lease recovery experiment
    // ═══════════════════════════════════════════════════════════════
    private static void runLeaseRecoveryExperiment(
            String host, int port, int payloadBytes,
            int channels, int maxInflight, List<String> topics, int topicCount, String output,
            int numConsumers, int consumerBatch,
            int shardCount, String bufferPoolSize, int batchThreshold, int flushIntervalMs,
            int maxPoolSize, String jvmFlags, int repeatIndex, int targetRate,
            String gitSha,
            List<String> mysqlUrls, String mysqlUser, String mysqlPass
    ) throws Exception {

        truncateAllQueues(mysqlUrls, mysqlUser, mysqlPass);

        try (CpuSampler cpuSampler = new CpuSampler();
             ChannelPool pool = new ChannelPool(host, port, channels);
             CsvResultWriter csv = new CsvResultWriter(Path.of(output))) {

            cpuSampler.start();

            // Phase 1: Enqueue messages (30s burst to fill the queue)
            logger.info("═══ Lease Recovery Phase 1: Enqueue burst (30s) ═══");
            var generator = new OpenLoopLoadGenerator(
                    pool, targetRate, 0, 30, payloadBytes, maxInflight, topics);
            generator.run();

            // Phase 2: Start consumers that DON'T ack (they hold leases)
            logger.info("═══ Lease Recovery Phase 2: Consumers lease without ACK ═══");
            List<ConsumerWorker> noAckConsumers = new ArrayList<>();
            List<Thread> noAckThreads = new ArrayList<>();
            for (int i = 0; i < numConsumers; i++) {
                int offset = i * (topics.size() / Math.max(1, numConsumers));
                var worker = new ConsumerWorker(pool, topics, consumerBatch, 2000, false, 0, offset);
                var thread = new Thread(worker, "consumer-noack-" + i);
                thread.setDaemon(true);
                thread.start();
                noAckConsumers.add(worker);
                noAckThreads.add(thread);
            }

            // Wait for consumers to lease messages
            Thread.sleep(5000);
            long leasedCount = noAckConsumers.stream().mapToLong(ConsumerWorker::getTotalDequeued).sum();
            logger.info("Consumers leased {} messages without ACK", leasedCount);

            // Phase 3: Kill consumers (simulates SIGKILL)
            logger.info("═══ Lease Recovery Phase 3: Killing consumers ═══");
            long killTimeNs = System.nanoTime();
            noAckConsumers.forEach(ConsumerWorker::stop);
            noAckThreads.forEach(Thread::interrupt);
            for (Thread t : noAckThreads) t.join(3000);

            // Phase 4: Wait for lease expiration + reclaimer, then start new consumers
            logger.info("═══ Lease Recovery Phase 4: Waiting for redelivery ═══");
            // Lease duration is 30s by default, reclaimer interval is 5s.
            // So worst case redelivery = 30s (lease) + 5s (reclaimer scan) + ~1s (prefetch refill)
            // We wait up to 60s for a redelivery.

            ConsumerWorker recoveryWorker = new ConsumerWorker(pool, topics, consumerBatch, 5000, true, 0);
            Thread recoveryThread = new Thread(recoveryWorker, "consumer-recovery");
            recoveryThread.setDaemon(true);
            recoveryThread.start();

            // Poll until the recovery consumer gets messages or 60s timeout
            long timeoutNs = System.nanoTime() + 60_000_000_000L;
            while (recoveryWorker.getFirstDequeueAfterResumeNs() == 0
                    && System.nanoTime() < timeoutNs) {
                Thread.sleep(500);
            }

            long redeliveryTimeMs;
            if (recoveryWorker.getFirstDequeueAfterResumeNs() > 0) {
                redeliveryTimeMs = (recoveryWorker.getFirstDequeueAfterResumeNs() - killTimeNs) / 1_000_000;
                logger.info("═══ Time to redelivery: {} ms ═══", redeliveryTimeMs);
            } else {
                redeliveryTimeMs = -1;
                logger.warn("═══ No redelivery within 60s timeout ═══");
            }

            recoveryWorker.stop();
            recoveryThread.interrupt();
            recoveryThread.join(3000);

            List<Long> rowsPerShard = countRowsPerShard(mysqlUrls, mysqlUser, mysqlPass);
            long totalRows = rowsPerShard.stream().mapToLong(Long::longValue).sum();
            double observedShardSkew = 0.0;
            if (shardCount > 1 && totalRows > 0) {
                double even = (double) totalRows / shardCount;
                double maxDev = 0.0;
                for (long c : rowsPerShard) {
                    maxDev = Math.max(maxDev, Math.abs(c - even) / even);
                }
                observedShardSkew = maxDev;
            }
            String messagesPerShard = rowsPerShard.toString();
            double hostCpuPct = cpuSampler.getAverageCpuPct();

            // Create a minimal result for CSV (p99 = redelivery time)
            Histogram h = new Histogram(1, 60_000_000L, 3);
            if (redeliveryTimeMs > 0) {
                h.recordValue(redeliveryTimeMs * 1000); // record in μs
            }
            BenchmarkResult result = new BenchmarkResult(
                    h, leasedCount, redeliveryTimeMs < 0 ? 1 : 0,
                    60_000, // 60s window
                    0, "lease_recovery: " + redeliveryTimeMs + "ms",
                    topicCount, observedShardSkew, hostCpuPct, messagesPerShard
            );

            csv.writeRow(gitSha, shardCount, bufferPoolSize, batchThreshold, flushIntervalMs,
                    maxPoolSize, jvmFlags, targetRate, payloadBytes, "lease_recovery", repeatIndex, result);

            logger.info("Lease recovery experiment complete.");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Helpers
    // ═══════════════════════════════════════════════════════════════

    private static void truncateAllQueues(List<String> urls, String user, String pass) {
        for (String url : urls) {
            truncateQueue(url, user, pass);
        }
    }

    private static void truncateQueue(String url, String user, String pass) {
        logger.info("Purging queue_messages on {}...", url);
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("TRUNCATE TABLE queue_messages");
            logger.info("queue_messages truncated on {}.", url);
        } catch (Exception e) {
            logger.warn("TRUNCATE failed on {} (table may not exist yet): {}", url, e.getMessage());
        }
    }

    private static List<Long> countRowsPerShard(List<String> urls, String user, String pass) {
        List<Long> counts = new ArrayList<>();
        for (String url : urls) {
            try (Connection conn = DriverManager.getConnection(url, user, pass);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM queue_messages")) {
                if (rs.next()) {
                    counts.add(rs.getLong(1));
                } else {
                    counts.add(0L);
                }
            } catch (Exception e) {
                logger.warn("Failed to count rows on {}: {}", url, e.getMessage());
                counts.add(0L);
            }
        }
        return counts;
    }

    private static void printSummary(BenchmarkResult r, int targetRate) {
        logger.info("┌─────────────────────────────────────────┐");
        logger.info("│          BENCHMARK RESULTS              │");
        logger.info("├─────────────────────────────────────────┤");
        logger.info("│ Target rate     : {} msg/s", targetRate);
        logger.info("│ Achieved rate   : {} msg/s", String.format(Locale.ROOT, "%.0f", r.achievedThroughput()));
        logger.info("│ Dequeue rate    : {} msg/s", String.format(Locale.ROOT, "%.0f", r.dequeueThroughput()));
        logger.info("│ p50 latency     : {} ms", String.format(Locale.ROOT, "%.3f", r.p50Ms()));
        logger.info("│ p95 latency     : {} ms", String.format(Locale.ROOT, "%.3f", r.p95Ms()));
        logger.info("│ p99 latency     : {} ms", String.format(Locale.ROOT, "%.3f", r.p99Ms()));
        logger.info("│ p99.9 latency   : {} ms", String.format(Locale.ROOT, "%.3f", r.p999Ms()));
        logger.info("│ Error count     : {}", r.errorCount());
        logger.info("│ Topics          : {}", r.topicCount());
        logger.info("│ Shard skew      : {}", String.format(Locale.ROOT, "%.2f%%", r.observedShardSkew() * 100));
        logger.info("│ Host CPU        : {}", String.format(Locale.ROOT, "%.1f%%", r.hostCpuPct()));
        logger.info("│ Msgs per shard  : {}", r.messagesPerShard());
        logger.info("│ EXPLAIN Extra   : {}", r.explainExtra());
        logger.info("└─────────────────────────────────────────┘");
    }

    private static String resolveGitSha() {
        try {
            Process p = new ProcessBuilder("git", "rev-parse", "--short", "HEAD")
                    .redirectErrorStream(true)
                    .start();
            String sha = new String(p.getInputStream().readAllBytes()).trim();
            p.waitFor(5, TimeUnit.SECONDS);
            return sha.isEmpty() ? "unknown" : sha;
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> opts = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length) {
                opts.put(args[i].substring(2), args[i + 1]);
                i++;
            }
        }
        return opts;
    }

    private static String requireOpt(Map<String, String> opts, String key) {
        String val = opts.get(key);
        if (val == null || val.isBlank()) {
            logger.error("Required option missing: --{}", key);
            printUsage();
            System.exit(1);
        }
        return val;
    }

    private static String opt(Map<String, String> opts, String key, String def) {
        return opts.getOrDefault(key, def);
    }

    private static void printUsage() {
        System.err.println("""
                Usage: java -jar foqs-bench.jar --experiment <name> --target-rate <int> [options]
                
                Experiments: baseline, batch_sweep, backlog, lease_recovery, smoke
                
                Options:
                  --host <string>           gRPC server host (default: localhost)
                  --port <int>              gRPC server port (default: 8080)
                  --warmup <int>            warmup seconds (default: 60)
                  --duration <int>          measurement seconds (default: 180)
                  --payload-bytes <int>     payload size (default: 256)
                  --channels <int>          gRPC channel pool size (default: 8)
                  --max-inflight <int>      max concurrent requests (default: 2048)
                  --topic <string>          queue topic (default: bench-topic)
                  --output <path>           CSV output file (default: bench-results.csv)
                  --consumers <int>         consumer thread count (default: 4)
                  --consumer-batch <int>    dequeue batch size (default: 100)
                  --consumer-rate <int>     max dequeue rate/consumer, 0=unlimited (default: 0)
                  
                Metadata (CSV only, don't affect bench):
                  --shard-count <int>       (default: 1)
                  --buffer-pool-size <str>  (default: 4G)
                  --batch-threshold <int>   (default: 100)
                  --flush-interval-ms <int> (default: 10)
                  --max-pool-size <int>     (default: 20)
                  --jvm-flags <string>      (default: "")
                  --repeat-index <int>      (default: 0)
                  
                MySQL (for EXPLAIN/monitoring):
                  --mysql-url <string>      (default: jdbc:mysql://localhost:3306/foqs_shard_0)
                  --mysql-user <string>     (default: root)
                  --mysql-password <string> (default: root)
                """);
    }

    private static String padCenter(String s, int width) {
        if (s.length() >= width) return s;
        int pad = width - s.length();
        int left = pad / 2;
        int right = pad - left;
        return " ".repeat(left) + s + " ".repeat(right);
    }
}
