package project.khaihust.foqs.core.buffer.impl;

import lombok.extern.slf4j.Slf4j;
import project.khaihust.foqs.core.storage.ISingleShardQueueRepository;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LeaseReclaimer implements AutoCloseable {
    private final ISingleShardQueueRepository queueRepository;
    private final ScheduledExecutorService reclaimer;

    public LeaseReclaimer(ISingleShardQueueRepository queueRepository, int checkIntervalSeconds) {
        this.queueRepository = queueRepository;
        this.reclaimer = Executors.newSingleThreadScheduledExecutor(
                r -> {
                    Thread t = new Thread(r, "foqs-lease-reclaimer");
                    t.setDaemon(true);
                    return t;
                }
        );
        this.reclaimer.scheduleWithFixedDelay(
                this::reclaimLease,
                checkIntervalSeconds,
                checkIntervalSeconds,
                TimeUnit.SECONDS
        );
    }

    public void reclaimLease(){
        try {
            queueRepository.reclaimExpiredLeases();
        } catch (Exception e) {
            log.error("Error while reclaiming leases", e);
        }
    }
    @Override
    public void close() throws Exception {
        reclaimer.shutdown();
        try {
            if (!reclaimer.awaitTermination(5, TimeUnit.SECONDS)) {
                reclaimer.shutdownNow();
            }
        } catch (InterruptedException e) {
            reclaimer.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
