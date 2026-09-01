package project.khaihust.foqs.core.storage;

import project.khaihust.foqs.core.enums.MessageStatus;
import project.khaihust.foqs.core.models.EnqueueRequest;
import project.khaihust.foqs.core.models.EnqueueTask;
import project.khaihust.foqs.core.models.Message;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

public interface ISingleShardQueueRepository {

    void enqueueBatch(List<EnqueueTask> enqueueTasks) throws SQLException;
    List<Message> leaseMessages(String topic, int batchLimit, Duration leaseDuration) throws SQLException;
    int reclaimExpiredLeases() throws SQLException;
}
