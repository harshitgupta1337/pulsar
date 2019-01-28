package org.apache.pulsar.broker;

import java.util.Map;

import org.apache.pulsar.common.policies.data.NetworkCoordinate;

public class BundleLatencyData {

	private NetworkCoordinate brokerCoord;
	private Map<String, NetworkCoordinate> producerCoords;
	private Map<String, NetworkCoordinate> consumerCoords;
	
	public Map<String, NetworkCoordinate> getProducerCoords() {
		return producerCoords;
	}
	
	public Map<String, NetworkCoordinate> getConsumerCoords() {
		return consumerCoords;
	}
	
	public NetworkCoordinate getBrokerCoord() {
		return brokerCoord;
	}
}
