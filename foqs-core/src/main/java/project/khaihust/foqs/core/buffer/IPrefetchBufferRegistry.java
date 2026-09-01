package project.khaihust.foqs.core.buffer;

public interface IPrefetchBufferRegistry {
    IPrefetchBatch getOrCreateBuffer(String topic);

    void close();
}
