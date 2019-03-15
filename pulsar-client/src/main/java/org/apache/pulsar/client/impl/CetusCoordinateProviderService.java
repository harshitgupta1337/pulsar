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
package org.apache.pulsar.client.impl;

import static java.lang.String.format;

import com.google.common.collect.Lists;

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetNetworkCoordinateResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse.LookupType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetNetworkCoordinateResponse;
import org.apache.pulsar.common.policies.data.NetworkCoordinate;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CetusCoordinateProviderService {

    private final PulsarClientImpl client;
    protected volatile InetSocketAddress serviceAddress;
    private final boolean useTls;
    private final ExecutorService executor;
    private final ScheduledExecutorService coordinateSendService;

    public CetusCoordinateProviderService(PulsarClientImpl client, String serviceUrl, boolean useTls, ExecutorService executor) throws PulsarClientException {
        this.client = client;
        this.useTls = useTls;
        this.executor = executor;
        updateServiceUrl(serviceUrl);
        this.coordinateSendService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("cetus-coordinate-sender"));
        startCoordinateSendService();
    }

    void startCoordinateSendService() {
        int interval = 100;
        this.coordinateSendService.scheduleAtFixedRate(safeRun(() -> sendCoordinates()), 
                                                  interval, interval, TimeUnit.MILLISECONDS);
    }

    /*
    public void sendCoordinates() {
        client.getCnxPool().getConnection(serviceAddress).thenAccept(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf msg = Commands.newGetNetworkCoordinateResponse(client.createGetAllNetworkCoordinateResponse(requestId));
            //CommandGetNetworkCoordinateResponse.Builder commandGetNetworkCoordinateResponseBuilder = CommandGetNetworkCoordinateResponse.newBuilder();
            //commandGetNetworkCoordinateResponseBuilder.setRequestId(requestId);
            //CoordinateInfo.Builder coordinateInfoBuilder = CoordinateInfo.newBuilder();
            //coordinateInfoBuilder.setNodeType("consumer");
            //coordinateInfoBuilder.setNodeId(4);
            //coordinateInfoBuilder.set
            clientCnx.sendNetworkCoordinates(msg, requestId);
        }).exceptionally((e) -> {
            log.warn("{} failed to send network coordinate response: {}", e.getCause().getMessage(), e);
            return null;
            });
             
    }
    */

    public void sendCoordinates() {
        client.sendNetworkCoordinates();
    }

    public void updateServiceUrl(String serviceUrl) throws PulsarClientException {
        URI uri;
        try {
            uri = new URI(serviceUrl);

            // Don't attempt to resolve the hostname in DNS at this point. It will be done each time when attempting to
            // connect
            this.serviceAddress = InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort());
        } catch (Exception e) {
            log.error("Invalid service-url {} provided {}", serviceUrl, e.getMessage(), e);
            throw new PulsarClientException.InvalidServiceURL(e);
        }
    }
    
    

    private static final Logger log = LoggerFactory.getLogger(CetusCoordinateProviderService.class);
};
