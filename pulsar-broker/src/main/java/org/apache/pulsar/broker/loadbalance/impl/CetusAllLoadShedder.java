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
package org.apache.pulsar.broker.loadbalance.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy;
import org.apache.pulsar.broker.loadbalance.CetusBundleUnloadingStrategy;
import org.apache.pulsar.common.util.CoordinateUtil;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.CetusLatencyMonitoringData;
import org.apache.pulsar.policies.data.loadbalancer.CetusNetworkCoordinateData;
import org.apache.pulsar.common.policies.data.NetworkCoordinate;
import org.apache.pulsar.broker.loadbalance.CetusModularLoadManager;
import org.apache.pulsar.broker.loadbalance.BrokerChange;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.loadbalance.CetusLoadData;
import org.apache.pulsar.policies.data.loadbalancer.CetusCentroidBrokerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load shedding strategy which will attempt to unload bundles from brokers who are actually
 * closer to other brokers, until we have a better bundle shedding strategy
 * 
 * 
 * 
 * 
 */
public class CetusAllLoadShedder  implements CetusBundleUnloadingStrategy {

    private static final Logger log = LoggerFactory.getLogger(CetusLoadShedder.class);

    private final Multimap<String, BrokerChange> selectedBundleCache = ArrayListMultimap.create();

    public Multimap<String, BrokerChange> processCentroids(ConcurrentHashMap<String, CetusLatencyMonitoringData> brokerLatencyDataMap, ServiceConfiguration conf, String loadMgrAddress) {
        selectedBundleCache.clear();
        log.info("ALL_LOAD_SHEDDER LOAD MANAGER ADDR : {}", loadMgrAddress.split("//")[1]);
        log.info("ALL_LOAD_SHEDDER SelectedBundleCache: {}", selectedBundleCache.toString());
        log.info("ALL_LOAD_SHEDDER Finding Bundles to Unload: Brokers: {} ", brokerLatencyDataMap.entrySet());
        for(Map.Entry<String, CetusLatencyMonitoringData> entry : brokerLatencyDataMap.entrySet()) {
            log.info("ALL_LOAD_SHEDDER Checking CetusBrokerData for broker {}", entry.getKey());
            for(Map.Entry<String, NetworkCoordinate> topicEntry : entry.getValue().getBundleCentroidCoordinates().entrySet()) {
                log.info("ALL_LOAD_SHEDDER Adding bundle {} on broker {} for load shedding", topicEntry.getKey(), entry.getKey());
                selectedBundleCache.put(entry.getKey(), new BrokerChange(topicEntry.getKey(), null));
            }
        }
        log.info("Load Shedding Completed");
        return selectedBundleCache;
    }

    public Multimap<String, BrokerChange> processAllPairs(ConcurrentHashMap<String, CetusLatencyMonitoringData> brokerLatencyDataMap, ServiceConfiguration conf, String loadMgrAddress) {
        selectedBundleCache.clear();
        log.info("ALL_LOAD_SHEDDER LOAD MANAGER ADDR : {}", loadMgrAddress.split("//")[1]);
        log.info("ALL_LOAD_SHEDDER SelectedBundleCache: {}", selectedBundleCache.toString());
        log.info("ALL_LOAD_SHEDDER Finding Bundles to Unload: Brokers: {} ", brokerLatencyDataMap.entrySet());
        for(Map.Entry<String, CetusLatencyMonitoringData> entry : brokerLatencyDataMap.entrySet()) {
            log.info("ALL_LOAD_SHEDDER Checking CetusBrokerData for broker {}", entry.getKey());
            for(Map.Entry<String, CetusNetworkCoordinateData> topicEntry : entry.getValue().getBundleNetworkCoordinates().entrySet()) {
                log.info("ALL_LOAD_SHEDDER Adding bundle {} on broker {} for load shedding", topicEntry.getKey(), entry.getKey());
                selectedBundleCache.put(entry.getKey(), new BrokerChange(topicEntry.getKey(), null));
            }
        }
        log.info("Load Shedding Completed");
        return selectedBundleCache;
    }

    public Multimap<String, BrokerChange> findBundlesForUnloading(CetusLoadData cetusLoadData, ServiceConfiguration conf, String loadMgrAddress, String brokerSelStrategy) {
        if (brokerSelStrategy.equals("CentroidMin"))
            return processCentroids(cetusLoadData.getCetusBrokerLatencyDataMap(), conf, loadMgrAddress);
        else if (brokerSelStrategy.equals("CentroidSat"))
            return processCentroids(cetusLoadData.getCetusBrokerLatencyDataMap(), conf, loadMgrAddress);
        else if (brokerSelStrategy.equals("AllPairsMin"))
            return processAllPairs(cetusLoadData.getCetusBrokerLatencyDataMap(), conf, loadMgrAddress);
        else {
            throw new RuntimeException("Unsupported broker selection strategy "+brokerSelStrategy);
        }
    } 
}
