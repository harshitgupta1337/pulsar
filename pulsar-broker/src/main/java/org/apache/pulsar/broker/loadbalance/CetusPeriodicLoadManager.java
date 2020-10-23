package org.apache.pulsar.broker.loadbalance;

public interface CetusPeriodicLoadManager extends ModularLoadManager {
    public static int CETUS_LATENCY_BOUND_MS = 75000;
    public static int WARMUP_SECS = 60;
    public static String BROKER_LIST_LOC = "/tmp/brokerslist.txt";
    public static String TARGET_BROKER = "/tmp/targetbroker.txt";
}
