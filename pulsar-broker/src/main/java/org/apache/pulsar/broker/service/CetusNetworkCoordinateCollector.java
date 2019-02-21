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
package org.apache.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.Hashmap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.common.api.proto.PulsarApi.CommandGetNetworkCoordinate;
import org.apache.common.api.proto.PulsarApi.CommandGetNetworkCoordinateResponse;
import org.apache.pulsar.common.policies.data.NetworkCoordinate;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CetusNetworkCoordinateCollector {
    public static final Logger log = LoggerFactory.getLogger(CetusNetworkCoordinateCollector.class);

    private final ServerCnx serverCnx;
    private final ConcurrentLongHashMap<CompletableFuture<Producer>> producers;
    private final ConcurrentLongHashMap<CompletableFuture<Consumer>> consumers;

    public CetusNetworkCoordinateCollector() {
    } 

    

    @Override
    protected void handleGetNetworkCoordinateResponse(CommandGetNetworkCoordinateResponse commandGetNetworkCoordinateResponse) {

        long 

        if(log.isDebugEnabled()) {
            log.debug("Received CommandGetNetworkCoordinateResponse call");
        }

        long requestId = CommandGetNetworkCoordinateRespones.getRequestId();
        if(CommandGetNetworkCoordinateResponse.hasErrorCode())
        {
            log.debug("Error on Get Network Coordinate Response {}", CommandGetNetworkCoordinateResponse.getErrorCode());
        }
        
        if(CommandGetNetworkCoordinateResponse.has
    }
}
