package project.khaihust.foqs.core.storage;

import com.fasterxml.uuid.impl.UUIDUtil;
import lombok.RequiredArgsConstructor;
import project.khaihust.foqs.core.enums.MessageStatus;
import project.khaihust.foqs.core.models.EnqueueTask;
import project.khaihust.foqs.core.models.Message;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@RequiredArgsConstructor
public class SingleShardQueueRepository implements ISingleShardQueueRepository {
    private final DataSource dataSource;

    @Override
    public void enqueueBatch(List<EnqueueTask> enqueueTasks) throws SQLException {
        if (enqueueTasks == null || enqueueTasks.isEmpty()) {
            return;
        }

        var sql = "INSERT INTO queue_messages (id, topic, priority, payload, deliver_after) " +
                "VALUES (?, ?, ?, ?, ?)";

        try (var connection = dataSource.getConnection();
             var preparedStatement = connection.prepareStatement(sql)) {
            for (var task : enqueueTasks) {
                preparedStatement.setBytes(1, UUIDUtil.asByteArray(task.getMessageId()));
                preparedStatement.setString(2, task.getEnqueueRequest().getTopic());
                preparedStatement.setInt(3, task.getEnqueueRequest().getPriority());
                preparedStatement.setBytes(4, task.getEnqueueRequest().getPayload());
                preparedStatement.setTimestamp(5, new Timestamp(task.getEnqueueRequest().getDeliverAfter()));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();

            for (var enqueueTask : enqueueTasks) {
                enqueueTask.getFuture().complete(enqueueTask.getMessageId());
            }
        } catch (SQLException e) {
            throw new SQLException("Failed to enqueue batch messages", e);
        }

    }

    @Override
    public List<Message> leaseMessages(String topic, int batchLimit, Duration leaseDuration) throws SQLException {
        List<Message> messages = new ArrayList<>(batchLimit);
        Instant now = Instant.now();
        Instant leaseUntil = now.plus(leaseDuration);

        String selectSql = """
                    SELECT id, topic, priority, payload, deliver_after, retry_count, created_at
                    FROM queue_messages
                    WHERE topic = ? 
                      AND status = 0 
                      AND deliver_after <= ?
                    ORDER BY priority ASC, id ASC
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                """;

        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            var lockedIdsByBytes = new ArrayList<byte[]>(batchLimit);

            try (var preparedStatement = connection.prepareStatement(selectSql)) {
                preparedStatement.setString(1, topic);
                preparedStatement.setTimestamp(2, Timestamp.from(now));
                preparedStatement.setInt(3, batchLimit);
                try (var rs = preparedStatement.executeQuery()) {
                    while (rs.next()) {
                        var idBytes = rs.getBytes("id");

                        lockedIdsByBytes.add(idBytes);

                        messages.add(Message.builder()
                                .id(UUIDUtil.uuid(idBytes))
                                .topic(rs.getString("topic"))
                                .priority(rs.getInt("priority"))
                                .payload(rs.getBytes("payload"))
                                .status(MessageStatus.LEASED)
                                .deliverAfter(rs.getTimestamp("deliver_after").toInstant())
                                .leaseUntil(leaseUntil)
                                .retryCount(rs.getInt("retry_count"))
                                .createdAt(rs.getTimestamp("created_at").toInstant())
                                .build());
                    }
                }
            }

            if (lockedIdsByBytes.isEmpty()) {
                connection.commit();
                return messages;
            }

            String placeholders = String.join(",", Collections.nCopies(lockedIdsByBytes.size(), "?"));
            String updateSql = "UPDATE queue_messages SET status = 1, lease_until = ? WHERE id IN (" + placeholders + ")";

            try (var psUpdate = connection.prepareStatement(updateSql)) {
                psUpdate.setTimestamp(1, Timestamp.from(leaseUntil));
                for (int i = 0; i < lockedIdsByBytes.size(); i++) {
                    psUpdate.setBytes(i + 2, lockedIdsByBytes.get(i));
                }
                psUpdate.executeUpdate();
            }

            connection.commit();
        }

        return messages;
    }

    @Override
    public int reclaimExpiredLeases() throws SQLException {
        String sql = """
            UPDATE queue_messages 
            SET status = 0, lease_until = NULL, retry_count = retry_count + 1 
            WHERE status = 1 AND lease_until < ?
            LIMIT 1000
        """;

        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.from(Instant.now()));
            return ps.executeUpdate();
        }
    }

    @Override
    public List<UUID> ackMessages(List<UUID> messageIds) throws SQLException {
        if (messageIds == null || messageIds.isEmpty()) {
            return Collections.emptyList();
        }

        var sortedIds = messageIds.stream().distinct().sorted().toList();
        String placeholders = String.join(",", Collections.nCopies(sortedIds.size(), "?"));
        String selectSql = "SELECT id FROM queue_messages WHERE status = 1 AND id IN (" + placeholders + ") FOR UPDATE";

        List<UUID> acked = new ArrayList<>();
        try (var conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (var psSelect = conn.prepareStatement(selectSql)) {
                for (int i = 0; i < sortedIds.size(); i++) {
                    psSelect.setBytes(i + 1, UUIDUtil.asByteArray(sortedIds.get(i)));
                }
                try (var rs = psSelect.executeQuery()) {
                    while (rs.next()) {
                        acked.add(UUIDUtil.uuid(rs.getBytes("id")));
                    }
                }
            }

            if (!acked.isEmpty()) {
                acked.sort(null);
                String updatePlaceholders = String.join(",", Collections.nCopies(acked.size(), "?"));
                String updateSql = "UPDATE queue_messages SET status = 2, lease_until = NULL WHERE id IN (" + updatePlaceholders + ")";
                try (var psUpdate = conn.prepareStatement(updateSql)) {
                    for (int i = 0; i < acked.size(); i++) {
                        psUpdate.setBytes(i + 1, UUIDUtil.asByteArray(acked.get(i)));
                    }
                    psUpdate.executeUpdate();
                }
            }

            conn.commit();
        }
        return acked;
    }

    @Override
    public int nackMessages(List<UUID> messageIds, long retryDelayMs, int maxRetries) throws SQLException {
        if (messageIds == null || messageIds.isEmpty()) {
            return 0;
        }

        var sortedIds = messageIds.stream().distinct().sorted().toList();
        var nextDelivery = Instant.now().plusMillis(Math.max(0, retryDelayMs));
        var placeholders = String.join(",", Collections.nCopies(sortedIds.size(), "?"));

        var sql = """
            UPDATE queue_messages
            SET status = CASE 
                            WHEN retry_count + 1 >= ? THEN 3 
                            ELSE 0 
                         END,
                lease_until = NULL,
                deliver_after = ?,
                retry_count = retry_count + 1
            WHERE status = 1 AND id IN (
        """ + placeholders + ")";

        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(sql)) {

            ps.setInt(1, maxRetries);
            ps.setTimestamp(2, Timestamp.from(nextDelivery));

            for (int i = 0; i < sortedIds.size(); i++) {
                ps.setBytes(i + 3, UUIDUtil.asByteArray(sortedIds.get(i)));
            }

            return ps.executeUpdate();
        }
    }

}
