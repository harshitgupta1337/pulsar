package org.apache.pulsar.broker.loadbalance;

public interface CetusModularLoadManager extends ModularLoadManager {
    public static int CETUS_LATENCY_BOUND_MS = 50; // MMOG Latency Req
}
