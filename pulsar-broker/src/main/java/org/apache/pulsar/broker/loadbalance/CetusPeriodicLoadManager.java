package org.apache.pulsar.broker.loadbalance;

public interface CetusPeriodicLoadManager extends ModularLoadManager {
    public static int CETUS_LATENCY_BOUND_MS = 75;
    public static int PERIOD_SEC = 30;
    public static int WARMUP_SECS = 60;
}
