package project.khaihust.foqs.core.storage;

import project.khaihust.foqs.core.enums.MessageStatus;
import project.khaihust.foqs.core.models.EnqueueRequest;
import project.khaihust.foqs.core.models.EnqueueTask;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

public interface ISingleShardQueueRepository {

    void enqueueBatch(List<EnqueueTask> enqueueTasks) throws SQLException;
}
