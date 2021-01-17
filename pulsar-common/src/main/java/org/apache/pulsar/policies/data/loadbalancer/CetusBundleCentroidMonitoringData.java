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
import org.apache.pulsar.common.util.CoordinateUtil;

import org.apache.pulsar.common.policies.data.NetworkCoordinate;
import org.apache.pulsar.policies.data.loadbalancer.CetusNetworkCoordinateData;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.pulsar.policies.data.loadbalancer.JSONWritable;

@JsonDeserialize(as = CetusBundleCentroidMonitoringData.class)
public class CetusBundleCentroidMonitoringData extends JSONWritable {
    public NetworkCoordinate producerCentroid;
    public NetworkCoordinate consumerCentroid;
    public double centroidDevn;
    public double worstCaseLatency;
    
    public CetusBundleCentroidMonitoringData() {
    }

    public CetusBundleCentroidMonitoringData(NetworkCoordinate brokerNC, CetusNetworkCoordinateData bundleNC) {
        this.producerCentroid = bundleNC.getProducerCentroid();
        this.consumerCentroid = bundleNC.getConsumerCentroid();
        
        this.centroidDevn = 0;
        this.worstCaseLatency = 0;
        double maxDevn = -1, maxBrokerDist = -1;
        for (Map.Entry<String, NetworkCoordinate> e : bundleNC.getProducerCoordinates().entrySet()) {
            double dist = CoordinateUtil.calculateDistance(this.producerCentroid, e.getValue());
            double brokerDist = CoordinateUtil.calculateDistance(brokerNC, e.getValue());
            if (maxDevn < 0 || maxDevn < dist)
                maxDevn = dist;
            if (maxBrokerDist < 0 || maxBrokerDist < brokerDist)
                maxBrokerDist = brokerDist;
        }
        this.centroidDevn += (maxDevn>=0)?maxDevn:0;
        this.worstCaseLatency += (maxBrokerDist>=0)?maxBrokerDist:0;

        maxDevn = -1; maxBrokerDist = -1;
        for (Map.Entry<String, NetworkCoordinate> e : bundleNC.getConsumerCoordinates().entrySet()) {
            double dist = CoordinateUtil.calculateDistance(this.consumerCentroid, e.getValue());
            double brokerDist = CoordinateUtil.calculateDistance(brokerNC, e.getValue());
            if (maxDevn < 0 || maxDevn < dist)
                maxDevn = dist;
            if (maxBrokerDist < 0 || maxBrokerDist < brokerDist)
                maxBrokerDist = brokerDist;
        }
        this.centroidDevn += (maxDevn>=0)?maxDevn:0;
        this.worstCaseLatency += (maxBrokerDist>=0)?maxBrokerDist:0;
    }

    public CetusBundleCentroidMonitoringData (CetusBundleCentroidMonitoringData c) {
        this.centroidDevn = c.getCentroidDevn();
        this.worstCaseLatency = c.getWorstCaseLatency();
        this.producerCentroid = c.getProducerCentroid();
        this.consumerCentroid = c.getConsumerCentroid();
    }

    public NetworkCoordinate getProducerCentroid() {
        return producerCentroid;
    }

    public NetworkCoordinate getConsumerCentroid() {
        return consumerCentroid;
    }

    public void setProducerCentroid(NetworkCoordinate n) {
        this.producerCentroid = n;
    }

    public void setConsumerCentroid(NetworkCoordinate n) {
        this.consumerCentroid = n;
    }

    public double getCentroidDevn () {
        return this.centroidDevn;
    }

    public double getWorstCaseLatency() {
        return this.worstCaseLatency;
    }

    public void setCentroidDevn(double centroidDevn) {
        this.centroidDevn = centroidDevn;
    }

    public void setWorstCaseLatency(double l) {
        this.worstCaseLatency = l;
    }
}
