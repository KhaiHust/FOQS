package project.khaihust.foqs.bench;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Captures EXPLAIN output for the lease query.
 *
 * The critical query is:
 *   SELECT ... FROM queue_messages
 *   WHERE topic = ? AND status = 0 AND deliver_after <= ?
 *   ORDER BY priority ASC, id ASC
 *   LIMIT ? FOR UPDATE SKIP LOCKED
 *
 * We want to know if "Using filesort" appears in the Extra column
 * under backlog conditions (when the working set spills past the
 * InnoDB buffer pool). idx_fetch_priority is designed to prevent
 * filesort, but if MySQL's optimizer chooses a different plan under
 * memory pressure, we need to detect it.
 */
public class ExplainCapture {
    private static final Logger logger = LoggerFactory.getLogger(ExplainCapture.class);

    private static final String EXPLAIN_SQL =
            "EXPLAIN SELECT id, topic, priority, payload, deliver_after, retry_count, created_at " +
            "FROM queue_messages " +
            "WHERE topic = ? AND status = 0 AND deliver_after <= ? " +
            "ORDER BY priority ASC, id ASC " +
            "LIMIT ?";

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public ExplainCapture(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    /**
     * Runs EXPLAIN and returns a summary string containing key, rows, and Extra fields.
     * Returns "N/A" on failure.
     */
    public String capture() {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(EXPLAIN_SQL)) {

            ps.setString(1, "bench-topic");
            ps.setTimestamp(2, Timestamp.from(Instant.now()));
            ps.setInt(3, 100);

            try (ResultSet rs = ps.executeQuery()) {
                StringBuilder sb = new StringBuilder();
                while (rs.next()) {
                    String type = rs.getString("type");
                    String possibleKeys = rs.getString("possible_keys");
                    String key = rs.getString("key");
                    String rows = rs.getString("rows");
                    String extra = rs.getString("Extra");
                    sb.append(String.format("type=%s key=%s possible_keys=%s rows=%s Extra=%s",
                            type, key, possibleKeys, rows, extra));
                }
                String result = sb.toString();
                logger.info("EXPLAIN result: {}", result);
                return result;
            }
        } catch (Exception e) {
            logger.warn("EXPLAIN capture failed: {}", e.getMessage());
            return "N/A (" + e.getMessage() + ")";
        }
    }

    /**
     * Checks whether the EXPLAIN plan contains "Using filesort".
     */
    public boolean hasFilesort() {
        return capture().contains("Using filesort");
    }
}
