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
                      AND deliver_after <= NOW(3)
                    ORDER BY priority ASC, id ASC
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                """;

        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            var lockedIdsByBytes = new ArrayList<byte[]>(batchLimit);

            try (var preparedStatement = connection.prepareStatement(selectSql)) {
                preparedStatement.setString(1, topic);
                preparedStatement.setInt(2, batchLimit);
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
            WHERE status = 1 AND lease_until < NOW(3)
        """;

        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement(sql)) {
            return ps.executeUpdate();
        }
    }


}
