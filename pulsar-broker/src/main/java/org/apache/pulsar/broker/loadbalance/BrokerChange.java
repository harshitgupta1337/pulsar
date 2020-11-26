package org.apache.pulsar.broker.loadbalance;

public class BrokerChange {
    public String bundle;
    public String nextBroker;

    public BrokerChange(String bund, String next) {
        this.bundle = bund;
        this.nextBroker = next;
    }
}
