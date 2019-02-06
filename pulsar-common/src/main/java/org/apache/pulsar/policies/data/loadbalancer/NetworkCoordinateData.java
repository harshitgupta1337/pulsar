package org.apache.pulsar.policies.data.loadbalancer;

import java.util.List;
import java.util.Map;

import org.apache.pulsar.common.policies.data.NetworkCoordinate;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/** 
 * This a per-broker data structure. All bundles/topics hosted on this broker have coordinates for their producers and consumers provided here
 * @author harshitg
 *
 */
@JsonDeserialize(as = NetworkCoordinate.class)
public class NetworkCoordinateData extends JSONWritable {
    private NetworkCoordinate brokerNwCoordinate;
    
    /**
     * Maps bundle/topic names to network coordinates of producers 
     */
    private Map<String, List<NetworkCoordinate>> producerNwCoordinates;
    
    /**
     * Maps bundle/topic names to network coordinates of consumers 
     */
    private Map<String, List<NetworkCoordinate>> consumerNwCoordinates;
    
    public NetworkCoordinate getBrokerNwCoordinate() {
        return brokerNwCoordinate;
    }
    public void setBrokerNwCoordinate(NetworkCoordinate brokerNwCoordinate) {
        this.brokerNwCoordinate = brokerNwCoordinate;
    }
    public Map<String, List<NetworkCoordinate>> getProducerNwCoordinates() {
        return producerNwCoordinates;
    }
    public void setProducerNwCoordinates(
            Map<String, List<NetworkCoordinate>> producerNwCoordinates) {
        this.producerNwCoordinates = producerNwCoordinates;
    }
    public Map<String, List<NetworkCoordinate>> getConsumerNwCoordinates() {
        return consumerNwCoordinates;
    }
    public void setConsumerNwCoordinates(
            Map<String, List<NetworkCoordinate>> consumerNwCoordinates) {
        this.consumerNwCoordinates = consumerNwCoordinates;
    }
    
    
}
