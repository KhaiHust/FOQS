package project.khaihust.foqs.bench;

import org.HdrHistogram.Histogram;

/**
 * Immutable result of a single benchmark run.
 * All latencies are in microseconds as recorded by HdrHistogram.
 */
public record BenchmarkResult(
        /** Snapshot of the latency histogram (values in μs). */
        Histogram histogram,
        /** Total successful enqueue RPCs during the measurement window. */
        long successCount,
        /** Total failed enqueue RPCs during the measurement window. */
        long errorCount,
        /** Measurement window duration in milliseconds (excludes warmup). */
        long measurementDurationMs,
        /** Consumer-side achieved throughput (msgs/sec), 0 if no consumers. */
        double dequeueThroughput,
        /** EXPLAIN Extra field for the lease query at capture time. */
        String explainExtra
) {
    /** Achieved enqueue throughput in msgs/sec. */
    public double achievedThroughput() {
        return (successCount * 1000.0) / measurementDurationMs;
    }

    /** p50 latency in milliseconds. */
    public double p50Ms() {
        return histogram.getValueAtPercentile(50.0) / 1000.0;
    }

    /** p95 latency in milliseconds. */
    public double p95Ms() {
        return histogram.getValueAtPercentile(95.0) / 1000.0;
    }

    /** p99 latency in milliseconds. */
    public double p99Ms() {
        return histogram.getValueAtPercentile(99.0) / 1000.0;
    }

    /** p99.9 latency in milliseconds. */
    public double p999Ms() {
        return histogram.getValueAtPercentile(99.9) / 1000.0;
    }
}
