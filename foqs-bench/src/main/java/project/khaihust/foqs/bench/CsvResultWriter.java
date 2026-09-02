package project.khaihust.foqs.bench;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * Appends one CSV row per benchmark run.
 *
 * Thread-safe: synchronized on the writer.
 * Creates the file with header if it doesn't exist.
 */
public class CsvResultWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CsvResultWriter.class);

    public static final String HEADER =
            "gitSha,shardCount,bufferPoolSize,batchThreshold,flushIntervalMs," +
            "maxPoolSize,jvmFlags,targetRate,payloadBytes,timestamp," +
            "experiment,achievedThroughput,p50,p95,p99,p999," +
            "errorCount,repeatIndex,dequeueThroughput,explainExtra";

    private final Writer writer;

    public CsvResultWriter(Path outputPath) throws IOException {
        boolean fileExists = Files.exists(outputPath);
        this.writer = Files.newBufferedWriter(outputPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
        if (!fileExists) {
            writer.write(HEADER + "\n");
            writer.flush();
            logger.info("Created CSV output: {}", outputPath);
        } else {
            logger.info("Appending to existing CSV: {}", outputPath);
        }
    }

    /**
     * Writes one row to the CSV. All latency values in milliseconds.
     */
    public synchronized void writeRow(
            String gitSha,
            int shardCount,
            String bufferPoolSize,
            int batchThreshold,
            int flushIntervalMs,
            int maxPoolSize,
            String jvmFlags,
            int targetRate,
            int payloadBytes,
            String experiment,
            int repeatIndex,
            BenchmarkResult result
    ) throws IOException {
        String timestamp = Instant.now().toString();

        // Quote fields that may contain commas or special chars
        String safeJvmFlags = quote(jvmFlags);
        String safeExplain = quote(result.explainExtra() != null ? result.explainExtra() : "");

        String row = String.join(",",
                gitSha,
                String.valueOf(shardCount),
                bufferPoolSize,
                String.valueOf(batchThreshold),
                String.valueOf(flushIntervalMs),
                String.valueOf(maxPoolSize),
                safeJvmFlags,
                String.valueOf(targetRate),
                String.valueOf(payloadBytes),
                timestamp,
                experiment,
                String.format("%.2f", result.achievedThroughput()),
                String.format("%.3f", result.p50Ms()),
                String.format("%.3f", result.p95Ms()),
                String.format("%.3f", result.p99Ms()),
                String.format("%.3f", result.p999Ms()),
                String.valueOf(result.errorCount()),
                String.valueOf(repeatIndex),
                String.format("%.2f", result.dequeueThroughput()),
                safeExplain
        );

        writer.write(row + "\n");
        writer.flush();
        logger.info("CSV row: experiment={}, rate={}, achieved={}, p99={}ms, errors={}",
                experiment, targetRate,
                String.format("%.0f", result.achievedThroughput()),
                String.format("%.3f", result.p99Ms()),
                result.errorCount());
    }

    /** RFC 4180 quoting: wrap in double-quotes, escape internal quotes. */
    private static String quote(String value) {
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    @Override
    public synchronized void close() throws IOException {
        writer.close();
    }
}
