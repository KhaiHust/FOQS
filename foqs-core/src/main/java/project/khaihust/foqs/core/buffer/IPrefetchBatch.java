package project.khaihust.foqs.core.buffer;

import project.khaihust.foqs.core.models.Message;

import java.time.Duration;
import java.util.List;

public interface IPrefetchBatch {
    public List<Message> pollBatch(int maxCount, Duration timeout) throws InterruptedException;
}
