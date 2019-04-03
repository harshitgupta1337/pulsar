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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.PulsarHandler;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetNetworkCoordinate;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetNetworkCoordinateResponse;
import org.apache.pulsar.common.policies.data.NetworkCoordinate;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.pulsar.policies.data.loadbalancer.JSONWritable;

@JsonDeserialize(as = CetusNetworkCoordinateData.class)
public class CetusNetworkCoordinateData extends JSONWritable {
    public static final Logger log = LoggerFactory.getLogger(CetusNetworkCoordinateData.class);

    private ConcurrentHashMap<Long, NetworkCoordinate> producerCoordinates;
    private ConcurrentHashMap<Long, NetworkCoordinate> consumerCoordinates;

    public CetusNetworkCoordinateData() {
        producerCoordinates = new ConcurrentHashMap<Long, NetworkCoordinate>(16,1);
        consumerCoordinates = new ConcurrentHashMap<Long, NetworkCoordinate>(16,1);
    } 

    public void putConsumerCoordinate(long nodeId, NetworkCoordinate coordinate) { 
        consumerCoordinates.put(nodeId, coordinate);
    }

    public void putProducerCoordinate(long nodeId, NetworkCoordinate coordinate) {
        producerCoordinates.put(nodeId, coordinate);
    }

    public NetworkCoordinate getConsumerCoordinate(long nodeId) {
        return consumerCoordinates.get(nodeId);
    }

    public NetworkCoordinate getProducerCoordinate(long nodeId) {
        return producerCoordinates.get(nodeId);
    }

    public ConcurrentHashMap<Long, NetworkCoordinate> getProducerCoordinates() {
        return producerCoordinates;
    }
    
    public void setProducerCoordinates(ConcurrentHashMap<Long, NetworkCoordinate> producerCoordinates) {
        this.producerCoordinates = producerCoordinates;
    }

    public void setConsumerCoordinates(ConcurrentHashMap<Long, NetworkCoordinate> consumerCoordinates) {
        this.consumerCoordinates = consumerCoordinates;
    }

    public ConcurrentHashMap<Long, NetworkCoordinate> getConsumerCoordinates() { 
        return consumerCoordinates;
    }
    
}
