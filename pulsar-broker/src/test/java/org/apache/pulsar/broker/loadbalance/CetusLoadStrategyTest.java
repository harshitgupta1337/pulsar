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
package org.apache.pulsar.broker.loadbalance;

import static org.testng.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared.BrokerTopicLoadingPredicate;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.CetusModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceAllocationPolicies;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.admin.PulsarAdmin;
import java.util.Map;
import java.util.Optional;
import java.net.URI;

import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageBrokerData;
import org.apache.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.CetusBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.CetusNetworkCoordinateData;
import org.apache.pulsar.common.policies.data.NetworkCoordinate;
import org.apache.pulsar.broker.service.BrokerService;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CetusLoadStrategyTest {
    private LocalBookkeeperEnsemble bkEnsemble;

    protected static final int ASYNC_EVENT_COMPLETION_WAIT = 100;
    private boolean isTcpLookup = true;

    private URL url1;
    private PulsarService pulsar1;
    private PulsarAdmin admin1;

    private URL url2;
    private PulsarService pulsar2;
    private PulsarAdmin admin2;

    private String primaryHost;
    private String secondaryHost;

    private NamespaceBundleFactory nsFactory;

    private PulsarClient pulsarClient;

    private CetusModularLoadManagerImpl primaryLoadManager;
    private CetusModularLoadManagerImpl secondaryLoadManager;

    private ExecutorService executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>());

    private final int ZOOKEEPER_PORT = PortManager.nextFreePort();
    private final int PRIMARY_BROKER_WEBSERVICE_PORT = PortManager.nextFreePort();
    private final int SECONDARY_BROKER_WEBSERVICE_PORT = PortManager.nextFreePort();
    private final int PRIMARY_BROKER_PORT = PortManager.nextFreePort();
    private final int SECONDARY_BROKER_PORT = PortManager.nextFreePort();
    private static final Logger log = LoggerFactory.getLogger(ModularLoadManagerImplTest.class);

    static {
        System.setProperty("test.basePort", "16100");
    }

    // Invoke non-overloaded method.
    private Object invokeSimpleMethod(final Object instance, final String methodName, final Object... args)
            throws Exception {
        for (Method method : instance.getClass().getDeclaredMethods()) {
            if (method.getName().equals(methodName)) {
                method.setAccessible(true);
                return method.invoke(instance, args);
            }
        }
        throw new IllegalArgumentException("Method not found: " + methodName);
    }

    private static Object getField(final Object instance, final String fieldName) throws Exception {
        final Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(instance);
    }

    private static void setField(final Object instance, final String fieldName, final Object value) throws Exception {
        final Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(instance, value);
    }

    @BeforeMethod
    void setup() throws Exception {

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, ZOOKEEPER_PORT, () -> PortManager.nextFreePort());
        bkEnsemble.start();

        // Start broker 1
        ServiceConfiguration config1 = new ServiceConfiguration();
        config1.setLoadManagerClassName(CetusModularLoadManagerImpl.class.getName());
        config1.setClusterName("use");
        config1.setWebServicePort(PRIMARY_BROKER_WEBSERVICE_PORT);
        config1.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        config1.setBrokerServicePort(PRIMARY_BROKER_PORT);
        pulsar1 = new PulsarService(config1);

        pulsar1.start();

        primaryHost = String.format("%s:%d", InetAddress.getLocalHost().getHostName(), PRIMARY_BROKER_WEBSERVICE_PORT);
        url1 = new URL("http://127.0.0.1" + ":" + PRIMARY_BROKER_WEBSERVICE_PORT);
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();

        // Start broker 2
        ServiceConfiguration config2 = new ServiceConfiguration();
        config2.setLoadManagerClassName(CetusModularLoadManagerImpl.class.getName());
        config2.setClusterName("use");
        config2.setWebServicePort(SECONDARY_BROKER_WEBSERVICE_PORT);
        config2.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        config2.setBrokerServicePort(SECONDARY_BROKER_PORT);
        pulsar2 = new PulsarService(config2);
        secondaryHost = String.format("%s:%d", InetAddress.getLocalHost().getHostName(),
                SECONDARY_BROKER_WEBSERVICE_PORT);

        pulsar2.start();

        url2 = new URL("http://127.0.0.1" + ":" + SECONDARY_BROKER_WEBSERVICE_PORT);
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();

        primaryLoadManager = (CetusModularLoadManagerImpl) getField(pulsar1.getLoadManager().get(), "loadManager");
        secondaryLoadManager = (CetusModularLoadManagerImpl) getField(pulsar2.getLoadManager().get(), "loadManager");
        nsFactory = new NamespaceBundleFactory(pulsar1, Hashing.crc32());
        Thread.sleep(100);
        admin1.clusters().createCluster("use", new ClusterData(url1.toString()));
        admin1.tenants().createTenant("prop", new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet("use")));
        admin1.namespaces().createNamespace("prop/ns-abc");
        admin1.namespaces().setNamespaceReplicationClusters("prop/ns-abc", Sets.newHashSet("use"));
        startClient();
    }

    @AfterMethod
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        executor.shutdown();

        admin1.close();
        admin2.close();

        pulsar2.close();
        pulsar1.close();

        pulsarClient.close();

        bkEnsemble.stop();
    }

    private NamespaceBundle makeBundle(final String property, final String cluster, final String namespace) {
        return nsFactory.getBundle(NamespaceName.get(property, cluster, namespace),
                Range.range(NamespaceBundles.FULL_LOWER_BOUND, BoundType.CLOSED, NamespaceBundles.FULL_UPPER_BOUND,
                        BoundType.CLOSED));
    }

    private NamespaceBundle makeBundle(final String all) {
        return makeBundle(all, all, all);
    }

    private String mockBundleName(final int i) {
        return String.format("%d/%d/%d/0x00000000_0xffffffff", i, i, i);
    }

    protected final void startClient() throws Exception {
        String lookupUrl = url1.toString();
        if(isTcpLookup) {
            lookupUrl = new URI("pulsar://localhost:" + PRIMARY_BROKER_PORT).toString();
        }
        pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString()).statsInterval(1, TimeUnit.SECONDS).build();
    }


    @Test
    public void testCetusLoadShedding() throws Exception {
        final String topicName = "non-persistent://prop/ns-abc/coordinateTopic";
        final String bundleName = pulsar1.getNamespaceService().getBundle(TopicName.get(topicName)).toString();
        final String subName = "successSub";
        double[] coordinateVector3 = new double[]{10,10,10,10,10,10,10,10};
        NetworkCoordinate coordinate3 = new NetworkCoordinate(0,0,0, coordinateVector3);
        pulsar2.getCetusBrokerData().setBrokerNwCoordinate(coordinate3);

        double[] coordinateVector = new double[]{1,1,1,1,1,1,1,1};
        NetworkCoordinate coordinate = new NetworkCoordinate(1,1,1, coordinateVector);
        NetworkCoordinate coordinate2 = new NetworkCoordinate(2,2,2, coordinateVector);

        pulsar1.getCetusBrokerData().setBrokerNwCoordinate(coordinate);
 
        // Setup Producers and Consumers
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
        BrokerService brokerService1 = pulsar1.getBrokerService();
        
                   
        log.info("Got Service");

        
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

        Thread.sleep(5000);

        assertTrue(primaryLoadManager.getAvailableBrokers().size() == 2);
        for(String brokers: primaryLoadManager.getAvailableBrokers()) {
            log.info("Active broker: {}", brokers);
        }
        assertTrue(pulsar1.getCetusBrokerData().getBundleNetworkCoordinates().containsKey(bundleName));

        consumer.setNetworkCoordinate(coordinate3);
        producer.setNetworkCoordinate(coordinate3);
        producer2.setNetworkCoordinate(coordinate3);

        Thread.sleep(500);
        log.info("Starting load shedding");
        primaryLoadManager.doLoadShedding();
        log.info("Completed load shedding");

        Thread.sleep(2500);

       assertTrue(pulsar2.getCetusBrokerData().getBundleNetworkCoordinates().containsKey(bundleName));

        

       
    }
}
 
