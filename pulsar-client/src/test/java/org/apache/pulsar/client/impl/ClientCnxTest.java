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


import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.mockito.Mockito.*;


import java.io.IOException;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Field;

import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.PulsarHandler;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.Test;
import org.apache.pulsar.common.api.proto.PulsarApi.BaseCommand;
import org.apache.pulsar.common.api.proto.PulsarApi.BaseCommand.Type;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetNetworkCoordinate;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetNetworkCoordinateResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;
import org.apache.pulsar.common.policies.data.NetworkCoordinate;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.client.impl.ClientChannelHelper;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.util.concurrent.TimeUnit;
import io.netty.util.concurrent.DefaultThreadFactory;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientCnxTest {
    protected EmbeddedChannel channel;
    //private ClientConfiguration clientConfig;
    public ClientCnx clientCnx;
    private ClientChannelHelper clientChannelHelper;
    private ClientConfigurationData clientConf;
    private EventLoopGroup clientEventLoop;
    

    // Needed for CETUS
    @BeforeMethod
    public void setup() throws Exception {
        clientChannelHelper = new ClientChannelHelper();
        clientEventLoop = EventLoopUtil.newEventLoopGroup(1, new DefaultThreadFactory("testNetworkCoordinateCommand")); 
        clientConf = new ClientConfigurationData();
    }

    protected void resetChannel() throws Exception {
        int MaxMessageSize = 5 * 1024 * 1024;
        if (channel != null && channel.isActive()) {
            clientCnx.close();
            channel.close().get();
        }
        clientEventLoop = EventLoopUtil.newEventLoopGroup(1, new DefaultThreadFactory("testNetworkCoordinateCommand"));
        clientCnx = new ClientCnx(clientConf, clientEventLoop);
        
        //serverCnx.authRole = "";
        channel = new EmbeddedChannel(new LengthFieldBasedFrameDecoder(MaxMessageSize, 0, 4, 0, 4), clientCnx);
        getResponse();
    }
    
    protected Object getResponse() throws Exception {
        // Wait at most for 10s to get a response
        final long sleepTimeMs = 10;
        final long iterations = TimeUnit.SECONDS.toMillis(10) / sleepTimeMs;
        for (int i = 0; i < iterations; i++) {
            if (!channel.outboundMessages().isEmpty()) {
                Object outObject = channel.outboundMessages().remove();
                return clientChannelHelper.getCommand(outObject);
                //return outObject;
            } else {
                Thread.sleep(sleepTimeMs);
            }
        }

        throw new IOException("Failed to get response from socket within 10s");
    }

    /*
    @Test
    public void testClientCnxTimeout() throws Exception {
        EventLoopGroup eventLoop = EventLoopUtil.newEventLoopGroup(1, new DefaultThreadFactory("testClientCnxTimeout"));
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setOperationTimeoutMs(10);
        ClientCnx cnx = new ClientCnx(conf, eventLoop);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        ChannelFuture listenerFuture = mock(ChannelFuture.class);
        when(listenerFuture.addListener(anyObject())).thenReturn(listenerFuture);
        when(ctx.writeAndFlush(anyObject())).thenReturn(listenerFuture);

        Field ctxField = PulsarHandler.class.getDeclaredField("ctx");
        ctxField.setAccessible(true);
        ctxField.set(cnx, ctx);
        try {
            cnx.newLookup(null, 123).get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException.TimeoutException);
        }
    }
    */

    //CETUS Network Coordinate Test
    @Test
    public void testNetworkCoordinateCommand() throws Exception {
        resetChannel();
        assertTrue(channel.isActive());
        

        CommandGetNetworkCoordinate.Builder commandGetNetworkCoordinateBuilder = CommandGetNetworkCoordinate.newBuilder();
        commandGetNetworkCoordinateBuilder.setRequestId(2);
        commandGetNetworkCoordinateBuilder.setNodeId(1);
        commandGetNetworkCoordinateBuilder.setNodeType("producer");

        CommandGetNetworkCoordinate commandGetNetworkCoordinate = commandGetNetworkCoordinateBuilder.build();
        // test client response to GET_NETWORK_COORDINATE
        ByteBuf serverCommand = Commands.serializeWithSize(BaseCommand.newBuilder().setType(Type.GET_NETWORK_COORDINATE)
            .setGetNetworkCoordinate(commandGetNetworkCoordinateBuilder));
        commandGetNetworkCoordinate.recycle();
        commandGetNetworkCoordinateBuilder.recycle();

        channel.writeInbound(serverCommand);
        //getResponse();
        CommandGetNetworkCoordinateResponse response = (CommandGetNetworkCoordinateResponse) getResponse();
        assertTrue(response.getRequestId() == 2);
        assertTrue(response.getErrorCode() != null);
        assertTrue(response.getErrorCode() == ServerError.ProducerNotFound);
                
    }
 
    @Test
    public void TestNetworkCoordinateConsumer() throws Exception {
        resetChannel();
        assertTrue(channel.isActive());
        ConsumerConfigurationData consumerConf = new ConsumerConfigurationData<>();
        ClientConfigurationData pulsarClientConf = new ClientConfigurationData();
        pulsarClientConf.setOperationTimeoutMs(100);
        pulsarClientConf.setStatsIntervalSeconds(0);
        //pulsarClientConf.setTopicName("non-persistent://tenant/ns1/my-topic");
        //pulsarClientConf.setServiceUrl("http://0.0.0.0");
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        //PulsarClientImpl pulsarClient = new PulsarClientImpl(pulsarClientConf, clientEventLoop);
        String topic = "non-persistent://tenant/ns1/my-topic";
        String subName = "successSub";
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        CompletableFuture<Consumer<ConsumerImpl>> subscribeFuture = new CompletableFuture<>();
        CompletableFuture<ClientCnx> clientCnxFuture = new CompletableFuture<ClientCnx>();

        consumerConf.setSubscriptionName("test-sub");
        when(pulsarClient.getConnection(anyString())).thenReturn(clientCnxFuture);
        when(pulsarClient.getConfiguration()).thenReturn(pulsarClientConf);
        ConsumerImpl<ConsumerImpl> consumer = new ConsumerImpl<ConsumerImpl>(pulsarClient,topic,consumerConf,executorService, -1, subscribeFuture, null, null);
        //Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName(subName).acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();
        clientCnx.registerConsumer(2, consumer);
        CommandGetNetworkCoordinate.Builder commandGetNetworkCoordinateBuilder = CommandGetNetworkCoordinate.newBuilder();
        commandGetNetworkCoordinateBuilder.setRequestId(2);
        commandGetNetworkCoordinateBuilder.setNodeId(2);
        commandGetNetworkCoordinateBuilder.setNodeType("consumer");

        CommandGetNetworkCoordinate commandGetNetworkCoordinate = commandGetNetworkCoordinateBuilder.build();
        // test client response to GET_NETWORK_COORDINATE
        ByteBuf serverCommand = Commands.serializeWithSize(BaseCommand.newBuilder().setType(Type.GET_NETWORK_COORDINATE)
            .setGetNetworkCoordinate(commandGetNetworkCoordinateBuilder));
        commandGetNetworkCoordinate.recycle();
        commandGetNetworkCoordinateBuilder.recycle();

        channel.writeInbound(serverCommand);
        //getResponse();
        CommandGetNetworkCoordinateResponse response = (CommandGetNetworkCoordinateResponse) getResponse();
        assertTrue(response.getRequestId() == 2);
        //assertTrue(response.getErrorCode() != null);
        //assertTrue(response.getErrorCode() == ServerError.ConsumerNotFound);
        assertTrue(response.getHeight() == 0);
        assertTrue(response.getError() == 0);
        assertTrue(response.getAdjustment() == 0);
        assertTrue(response.getCoordinates(0).getCoordinate() == 0);
        
    }
}
