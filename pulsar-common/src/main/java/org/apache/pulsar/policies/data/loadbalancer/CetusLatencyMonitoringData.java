/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.policies.data.loadbalancer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pulsar.common.policies.data.NetworkCoordinate;
import org.apache.pulsar.policies.data.loadbalancer.CetusNetworkCoordinateData;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.pulsar.policies.data.loadbalancer.JSONWritable;

@JsonDeserialize(as = CetusLatencyMonitoringData.class)
public class CetusLatencyMonitoringData extends JSONWritable {

    private NetworkCoordinate brokerNwCoordinate;
   
    private ConcurrentHashMap<String, NetworkCoordinate> bundleCentroidCoordinates;

    private ConcurrentHashMap<String, CetusNetworkCoordinateData> bundleNwCoordinates;
    
    public CetusLatencyMonitoringData() {
        this.brokerNwCoordinate = new NetworkCoordinate();
        this.bundleCentroidCoordinates = new ConcurrentHashMap<String, NetworkCoordinate>(16,1);
        this.bundleNwCoordinates = new ConcurrentHashMap<String, CetusNetworkCoordinateData>(16,1);
    }
    
    public CetusLatencyMonitoringData(CetusBrokerData cetusBrokerData, boolean centroid) {
        this.brokerNwCoordinate = cetusBrokerData.getBrokerNwCoordinate();
        this.bundleCentroidCoordinates = new ConcurrentHashMap<String, NetworkCoordinate>(16,1);
        this.bundleNwCoordinates = new ConcurrentHashMap<String, CetusNetworkCoordinateData>(16,1);

        if (centroid) {
            for(Map.Entry<String, CetusNetworkCoordinateData> topicEntry : cetusBrokerData.getBundleNetworkCoordinates().entrySet()) { 
                this.bundleCentroidCoordinates.put(topicEntry.getKey(), topicEntry.getValue().getProducerConsumerAvgCoordinate());
            }
        } else {
            this.bundleNwCoordinates = cetusBrokerData.getBundleNetworkCoordinates();
        }
    }

    public CetusLatencyMonitoringData(CetusLatencyMonitoringData cetusLatencyMonitoringData) {
        this.brokerNwCoordinate = cetusLatencyMonitoringData.getBrokerNwCoordinate();
        this.bundleCentroidCoordinates = cetusLatencyMonitoringData.getBundleCentroidCoordinates();
        this.bundleNwCoordinates = cetusLatencyMonitoringData.getBundleNetworkCoordinates();
    }
     
    public NetworkCoordinate getBrokerNwCoordinate() {
        return brokerNwCoordinate;
    }

    public void setBrokerNwCoordinate(NetworkCoordinate brokerNwCoordinate) {
        this.brokerNwCoordinate = brokerNwCoordinate;
    }

    public ConcurrentHashMap<String, NetworkCoordinate> getBundleCentroidCoordinates() {
        return bundleCentroidCoordinates;
    }

    public void setBundleCentroidCoordinates( ConcurrentHashMap<String, NetworkCoordinate> bundleCentroidCoordinates) {
        this.bundleCentroidCoordinates = bundleCentroidCoordinates;
    }
    
    public ConcurrentHashMap<String, CetusNetworkCoordinateData> getBundleNetworkCoordinates() {
        return bundleNwCoordinates;
    }

    public void setBundleNetworkCoordinates( ConcurrentHashMap<String, CetusNetworkCoordinateData> bundleNwCoordinates) {
        this.bundleNwCoordinates = bundleNwCoordinates;
    }
}
