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

@JsonDeserialize(as = CetusBrokerData.class)
public class CetusBrokerData extends JSONWritable {
    private NetworkCoordinate brokerNwCoordinate;
    
    private ConcurrentHashMap<String, CetusNetworkCoordinateData> topicNwCoordinates;
    
    public CetusBrokerData() {
        this.brokerNwCoordinate = new NetworkCoordinate();
        this.topicNwCoordinates = new ConcurrentHashMap<String, CetusNetworkCoordinateData>(16,1);
    }
    
    public CetusBrokerData(CetusBrokerData cetusBrokerData) {
        this.brokerNwCoordinate = cetusBrokerData.getBrokerNwCoordinate();
        this.topicNwCoordinates = cetusBrokerData.getTopicNetworkCoordinates();
    }
     
    public NetworkCoordinate getBrokerNwCoordinate() {
        return brokerNwCoordinate;
    }

    public void setBrokerNwCoordinate(NetworkCoordinate brokerNwCoordinate) {
        this.brokerNwCoordinate = brokerNwCoordinate;
    }

    public ConcurrentHashMap<String, CetusNetworkCoordinateData> getTopicNetworkCoordinates() {
        return topicNwCoordinates;
    }

    public void setTopicNetworkCoordinates( ConcurrentHashMap<String, CetusNetworkCoordinateData> topicNwCoordinates) {
        this.topicNwCoordinates = topicNwCoordinates;
    }
    
    
}
