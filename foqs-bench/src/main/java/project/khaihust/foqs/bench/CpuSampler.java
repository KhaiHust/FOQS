package project.khaihust.foqs.bench;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically samples host CPU usage percentage during benchmark runs.
 */
public class CpuSampler implements AutoCloseable {
    private final OperatingSystemMXBean osBean;
    private final ScheduledExecutorService scheduler;
    private final List<Double> samples = new CopyOnWriteArrayList<>();
    private volatile boolean running = true;

    public CpuSampler() {
        this.osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "foqs-cpu-sampler");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        // Prime the MXBean counter
        try {
            osBean.getCpuLoad();
        } catch (Throwable ignored) {
        }

        scheduler.scheduleAtFixedRate(() -> {
            if (!running) return;
            try {
                double load = osBean.getCpuLoad();
                if (load >= 0.0) {
                    samples.add(load * 100.0);
                }
            } catch (Throwable ignored) {
            }
        }, 500, 500, TimeUnit.MILLISECONDS);
    }

    public double getAverageCpuPct() {
        if (samples.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (double s : samples) {
            sum += s;
        }
        return sum / samples.size();
    }

    @Override
    public void close() {
        running = false;
        scheduler.shutdownNow();
    }
}
