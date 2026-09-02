package project.khaihust.foqs.bench;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Periodically samples InnoDB status during a benchmark run.
 *
 * Parses "History list length" from SHOW ENGINE INNODB STATUS
 * and writes timestamped rows to a dedicated CSV file.
 *
 * History list length indicates how many old row versions remain
 * un-purged. Under backlog with slow consumers, this grows and
 * signals MVCC pressure / undo log bloat.
 */
public class InnodbMonitor implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(InnodbMonitor.class);
    private static final Pattern HLL_PATTERN =
            Pattern.compile("History list length\\s+(\\d+)", Pattern.CASE_INSENSITIVE);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> future;
    private Writer csvWriter;

    public InnodbMonitor(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "innodb-monitor");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Start sampling every {@code intervalSeconds} seconds, writing to the given CSV path.
     */
    public void start(Path outputPath, int intervalSeconds) throws Exception {
        boolean exists = Files.exists(outputPath);
        this.csvWriter = Files.newBufferedWriter(outputPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
        if (!exists) {
            csvWriter.write("timestamp,historyListLength\n");
            csvWriter.flush();
        }

        this.future = scheduler.scheduleAtFixedRate(this::sample, 0, intervalSeconds, TimeUnit.SECONDS);
        logger.info("InnoDB monitor started: interval={}s, output={}", intervalSeconds, outputPath);
    }

    private void sample() {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW ENGINE INNODB STATUS")) {

            if (rs.next()) {
                String status = rs.getString("Status");
                Matcher matcher = HLL_PATTERN.matcher(status);
                if (matcher.find()) {
                    long hll = Long.parseLong(matcher.group(1));
                    String row = Instant.now() + "," + hll + "\n";
                    synchronized (this) {
                        if (csvWriter != null) {
                            csvWriter.write(row);
                            csvWriter.flush();
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("InnoDB status sample failed: {}", e.getMessage());
        }
    }

    /** Take a single snapshot and return the history list length, or -1 on failure. */
    public long snapshotHistoryListLength() {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW ENGINE INNODB STATUS")) {
            if (rs.next()) {
                Matcher m = HLL_PATTERN.matcher(rs.getString("Status"));
                if (m.find()) return Long.parseLong(m.group(1));
            }
        } catch (Exception e) {
            logger.warn("InnoDB snapshot failed: {}", e.getMessage());
        }
        return -1;
    }

    public void stop() {
        if (future != null) future.cancel(false);
    }

    @Override
    public void close() {
        stop();
        scheduler.shutdownNow();
        synchronized (this) {
            try {
                if (csvWriter != null) csvWriter.close();
            } catch (Exception e) {
                logger.warn("Error closing InnoDB monitor CSV: {}", e.getMessage());
            }
        }
    }
}
