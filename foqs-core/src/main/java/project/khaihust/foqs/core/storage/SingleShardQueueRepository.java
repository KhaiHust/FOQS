package project.khaihust.foqs.core.storage;

import lombok.RequiredArgsConstructor;
import project.khaihust.foqs.core.models.EnqueueTask;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

@RequiredArgsConstructor
public class SingleShardQueueRepository implements ISingleShardQueueRepository {
    private final DataSource dataSource;

    @Override
    public void enqueueBatch(List<EnqueueTask> enqueueTasks) throws SQLException {
        var sql = "INSERT INTO queue_messages (id, topic, priority, payload, deliver_after) " +
                "VALUES (?, ?, ?, ?, ?)";

        try (var connection = dataSource.getConnection();
             var preparedStatement = connection.prepareStatement(sql)) {
            for (var task : enqueueTasks) {
                preparedStatement.setObject(1, task.getMessageId());
                preparedStatement.setString(2, task.getEnqueueRequest().getTopic());
                preparedStatement.setInt(3, task.getEnqueueRequest().getPriority());
                preparedStatement.setBytes(4, task.getEnqueueRequest().getPayload());
                preparedStatement.setTimestamp(5, new Timestamp(task.getEnqueueRequest().getDeliverAfter()));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();

            for(var enqueueTask : enqueueTasks) {
                enqueueTask.getFuture().complete(enqueueTask.getMessageId());
            }
        } catch (SQLException e) {
            throw new SQLException("Failed to enqueue batch messages", e);
        }

    }


}
