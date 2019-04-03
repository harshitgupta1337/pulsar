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
import org.apache.pulsar.policies.data.loadbalancer.CetusNetworkCoordinateData;
import org.apache.pulsar.policies.data.loadbalancer.CetusBrokerData;
import org.apache.pulsar.common.naming.TopicName;
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
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(topicName).create(); 
        assertTrue(pulsarClient.producersCount() == 2);
        assertTrue(pulsarClient.consumersCount() == 1);
        long consumerId = consumer.getConsumerId();
        long producerId = producer.getProducerId();
        long producerId2 = producer2.getProducerId();
        log.info("Got Consumer Id:"+consumerId); 
        BrokerService brokerService = pulsar.getBrokerService();
           
        log.info("Got Service");

        double[] coordinateVector = new double[]{1,1,1,1,1,1,1,1};
        NetworkCoordinate coordinate = new NetworkCoordinate(1,1,1, coordinateVector);
        NetworkCoordinate coordinate2 = new NetworkCoordinate(2,2,2, coordinateVector);

        consumer.setNetworkCoordinate(coordinate);
        assertTrue(consumer.getNetworkCoordinate().getAdjustment() == 1);
        //URI uri = new URI(pulsarClientImpl.getConfiguration().getServiceUrl());
        //InetSocketAddress address = InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort());
        //ClientCnx cnx = pulsarClientImpl.getCnxPool().getConnection(address).getNow(null);
        //ConsumerImpl<?> consumerImpl = cnx.getConsumer(consumerId);
        //assertTrue(consumerImpl.getNetworkCoordinate().getAdjustment() != 0);


        producer.setNetworkCoordinate(coordinate);
        assertTrue(producer.getNetworkCoordinate().getAdjustment() == 1);

        producer2.setNetworkCoordinate(coordinate2);
        assertTrue(producer2.getNetworkCoordinate().getAdjustment() == 2);

        Thread.sleep(10000);

        assertTrue(brokerService.pulsar().getCetusBrokerData().getTopicNetworkCoordinates().get(topicName).getConsumerCoordinates().size() == 1);

        double adjustment = brokerService.pulsar().getCetusBrokerData().getTopicNetworkCoordinates().get(topicName).getConsumerCoordinate(consumerId).getAdjustment();

        assertTrue(adjustment == 1);

        adjustment = brokerService.pulsar().getCetusBrokerData().getTopicNetworkCoordinates().get(topicName).getProducerCoordinate(producerId).getAdjustment();

        assertTrue(adjustment == 1);

        adjustment = brokerService.pulsar().getCetusBrokerData().getTopicNetworkCoordinates().get(topicName).getProducerCoordinate(producerId2).getAdjustment();
        assertTrue(adjustment == 2);

        //assertTrue(brokerService.getNetworkCoordinateCollector().getConsumerCoordinate(consumerId).getAdjustment() == 1);

        String brokerZkPath = "/cetus/coordinate-data" + "/" + brokerService.pulsar().getAdvertisedAddress();

        CetusBrokerData cetusBrokerData = null;
        NetworkCoordinate consumerCoordinate = new NetworkCoordinate();
        NetworkCoordinate producerCoordinate = new NetworkCoordinate();
        NetworkCoordinate producerCoordinate2 = new NetworkCoordinate();

        if(pulsar.getZkClient().exists(brokerZkPath, null) != null)
        {
            cetusBrokerData = readJson(pulsar.getZkClient().getData(brokerZkPath, null, null), CetusBrokerData.class);
            log.info("Got Cetus Broker Data from: {}", brokerZkPath);
            log.info("Cetus Broker Topic Coordinate Map Size: {}", cetusBrokerData.getTopicNetworkCoordinates().size());
            for(Map.Entry<String, CetusNetworkCoordinateData> entry : cetusBrokerData.getTopicNetworkCoordinates().entrySet())
            {
                CetusNetworkCoordinateData cetusNetworkCoordinateData = readJson(pulsar.getZkClient().getData(getTopicZkPath(TopicName.get(entry.getKey()).getLookupName(), brokerService), null, null), CetusNetworkCoordinateData.class);
                cetusBrokerData.getTopicNetworkCoordinates().put(entry.getKey(), cetusNetworkCoordinateData);
                log.info("Producer Map Size in loop: {}, Topic: {}", cetusBrokerData.getTopicNetworkCoordinates().get(entry.getKey()).getProducerCoordinates().size(), entry.getKey());
                cetusNetworkCoordinateData.getProducerCoordinates().forEach((key, value) -> {
                    try {
                        NetworkCoordinate newCoordinate = readJson(pulsar.getZkClient().getData(getProducerZkPath(TopicName.get(entry.getKey()).getLookupName(), key, brokerService), null, null), NetworkCoordinate.class);
                        cetusNetworkCoordinateData.putProducerCoordinate(key, newCoordinate);
                    }
                    catch (Exception e) {
                    }
                });
                cetusNetworkCoordinateData.getConsumerCoordinates().forEach((key, value) -> {
                    try {
                        NetworkCoordinate newCoordinate = readJson(pulsar.getZkClient().getData(getConsumerZkPath(TopicName.get(entry.getKey()).getLookupName(), key, brokerService), null, null), NetworkCoordinate.class);
                    cetusNetworkCoordinateData.putConsumerCoordinate(key, newCoordinate);
                        log.info("Consumer coordinate : {}", newCoordinate.getAdjustment());
                    }
                    catch (Exception e){
                    }
                });
            }
            log.info("Cetus Broker Topic Coordinate Producer Map Size: {}", cetusBrokerData.getTopicNetworkCoordinates().get(topicName).getProducerCoordinates().size());

        }


        
        if(cetusBrokerData != null) {
            producerCoordinate = cetusBrokerData.getTopicNetworkCoordinates().get(topicName).getProducerCoordinate(producerId);

            consumerCoordinate = cetusBrokerData.getTopicNetworkCoordinates().get(topicName).getConsumerCoordinate(consumerId);        

            producerCoordinate2 = cetusBrokerData.getTopicNetworkCoordinates().get(topicName).getProducerCoordinate(producerId2);

            //log.info("Producer Coordinate Adjustment: " +producerCoordinate.getAdjustment());
        }
        else {
            log.info("No cetus broker data found !!!");
        }
        assertTrue(producerCoordinate.getAdjustment() == 1);

        assertTrue(consumerCoordinate.getAdjustment() == 1);

        assertTrue(producerCoordinate2.getAdjustment() == 2);
    }

    public String getTopicZkPath(final String topic, BrokerService brokerService) {
        return "/cetus/coordinate-data/" + brokerService.pulsar().getAdvertisedAddress()+ "/" + topic;
    }

    public String getProducerZkPath(final String topic, final long producerId, BrokerService brokerService) {
         return "/cetus/coordinate-data/" + brokerService.pulsar().getAdvertisedAddress()+ "/" + topic + "/producer/" + producerId;

    }

    public String getConsumerZkPath(final String topic, final long consumerId, BrokerService brokerService) {
         return "/cetus/coordinate-data/" + brokerService.pulsar().getAdvertisedAddress()+ "/" + topic + "/consumer/" + consumerId;

    }
}


