package project.khaihust.foqs.core.buffer.impl;

import lombok.extern.slf4j.Slf4j;
import project.khaihust.foqs.core.storage.ISingleShardQueueRepository;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LeaseReclaimer implements AutoCloseable {
    private final Collection<ISingleShardQueueRepository> repositories;
    private final ScheduledExecutorService reclaimer;

    public LeaseReclaimer(ISingleShardQueueRepository repository, int checkIntervalSeconds) {
        this(List.of(repository), checkIntervalSeconds);
    }

    public LeaseReclaimer(Collection<ISingleShardQueueRepository> repositories, int checkIntervalSeconds) {
        if (repositories == null || repositories.isEmpty()) {
            throw new IllegalArgumentException("repositories must not be null or empty");
        }
        this.repositories = List.copyOf(repositories);
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
        for(var repo : repositories){
            try {
                repo.reclaimExpiredLeases();
            } catch (Exception e) {
                log.error("Error while reclaiming leases", e);
            }
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
