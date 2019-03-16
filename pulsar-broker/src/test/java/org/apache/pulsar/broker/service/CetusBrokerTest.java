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

import static org.apache.pulsar.broker.cache.LocalZooKeeperCacheService.LOCAL_POLICIES_ROOT;
import static org.apache.pulsar.broker.web.PulsarWebResource.joinPath;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.BrokerServiceException.PersistenceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.NetworkCoordinate;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import lombok.Cleanup;

/**
 */
public class CetusBrokerTest extends BrokerTestBase {
    public static final Logger log = LoggerFactory.getLogger(CetusBrokerTest.class);
    
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        //log.info("Starting Setup");
        System.out.println("Starting Setup");
        super.baseSetup();
        log.info("Ending Setup");
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected static <T> T readJson(final byte[] data, final Class<T> clazz) throws IOException {
        return ObjectMapperFactory.getThreadLocal().readValue(data, clazz);
    } 

    @Test
    public void CoordinateReceivedTest() throws Exception {
        final String topicName = "non-persistent://prop/ns-abc/coordinateTopic";
        final String subName = "successSub";

        log.info("Got Topic Name");
        PulsarClientImpl pulsarClientImpl = (PulsarClientImpl) pulsarClient;
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName).acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();
        Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create(); 
        assertTrue(pulsarClient.producersCount() == 1);
        assertTrue(pulsarClient.consumersCount() == 1);
        long consumerId = consumer.getConsumerId();
        long producerId = producer.getProducerId();
        log.info("Got Consumer Id:"+consumerId); 
        BrokerService brokerService = pulsar.getBrokerService();
           
        log.info("Got Service");

        double[] coordinateVector = new double[]{1,1,1,1,1,1,1,1};
        NetworkCoordinate coordinate = new NetworkCoordinate(1,1,1, coordinateVector);

        consumer.setNetworkCoordinate(coordinate);
        assertTrue(consumer.getNetworkCoordinate().getAdjustment() == 1);
        //URI uri = new URI(pulsarClientImpl.getConfiguration().getServiceUrl());
        //InetSocketAddress address = InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort());
        //ClientCnx cnx = pulsarClientImpl.getCnxPool().getConnection(address).getNow(null);
        //ConsumerImpl<?> consumerImpl = cnx.getConsumer(consumerId);
        //assertTrue(consumerImpl.getNetworkCoordinate().getAdjustment() != 0);


        producer.setNetworkCoordinate(coordinate);
        assertTrue(producer.getNetworkCoordinate().getAdjustment() == 1);

        Thread.sleep(10000);

        assertTrue(brokerService.getNetworkCoordinateCollector().getConsumerCoordinates().size() == 1);

        double adjustment = brokerService.getNetworkCoordinateCollector().getConsumerCoordinate(consumerId).getAdjustment();

        assertTrue(adjustment == 1 || adjustment == -1);

        adjustment = brokerService.getNetworkCoordinateCollector().getProducerCoordinate(producerId).getAdjustment();

        assertTrue(adjustment == 1);

        //assertTrue(brokerService.getNetworkCoordinateCollector().getConsumerCoordinate(consumerId).getAdjustment() == 1);

        String consumerCoordinateZkPath = "/cetus/coordinate-data/consumer/" + consumerId;
        String producerCoordinateZkPath = "/cetus/coordinate-data/producer/" + producerId;

        NetworkCoordinate consumerCoordinate = new NetworkCoordinate();
        NetworkCoordinate producerCoordinate = new NetworkCoordinate();

        if(pulsar.getZkClient().exists(coordinateZkPath, null) != null)
        {
            consumerCoordinate = readJson(pulsar.getZkClient().getData(coordinateZkPath, null, null), NetworkCoordinate.class);
        }  
        
        assertTrue(consumerCoordinate.getAdjustment() == 1);
    }
}


