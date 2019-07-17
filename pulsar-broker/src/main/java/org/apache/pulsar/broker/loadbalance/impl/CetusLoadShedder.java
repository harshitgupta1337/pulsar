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
import org.apache.pulsar.policies.data.loadbalancer.CetusBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.CetusNetworkCoordinateData;
import org.apache.pulsar.broker.namespace.NamespaceService;
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
public class CetusLoadShedder  implements CetusBundleUnloadingStrategy {

    private static final Logger log = LoggerFactory.getLogger(CetusLoadShedder.class);

    private final Multimap<String, String> selectedBundleCache = ArrayListMultimap.create();

    public Multimap<String, String> findBundlesForUnloading(ConcurrentHashMap<String, CetusBrokerData> cetusBrokerDataMap, ServiceConfiguration conf, NamespaceService namespaceService) {
        selectedBundleCache.clear();
        log.info("SelectedBundleCache: {}", selectedBundleCache.toString());
        log.info("Finding Bundles to Unload: Brokers: {} ", cetusBrokerDataMap.entrySet());
        for(Map.Entry<String, CetusBrokerData> entry : cetusBrokerDataMap.entrySet()) {
            log.info("Load Shedding Bundles: {}", entry.getValue().getBundleNetworkCoordinates());
            for(Map.Entry<String, CetusNetworkCoordinateData> topicEntry : entry.getValue().getBundleNetworkCoordinates().entrySet()) {
                for(Map.Entry<String, CetusBrokerData> brokerEntry : cetusBrokerDataMap.entrySet()) {
                    log.info("[Cetus Load Shedder] Distance to broker: {}. Distance to Referenced Broker {}:  {} Topic Prod/Cons Coordinate: {} Broker Coordinate: {}", topicEntry.getValue().distanceToBroker(), brokerEntry.getKey(), CoordinateUtil.calculateDistance(topicEntry.getValue().getProducerConsumerAvgCoordinate(), brokerEntry.getValue().getBrokerNwCoordinate()), topicEntry.getValue().getProducerConsumerAvgCoordinate().getCoordinateVector(), brokerEntry.getValue().getBrokerNwCoordinate().getCoordinateVector()); 
                    if(CoordinateUtil.calculateDistance(topicEntry.getValue().getProducerConsumerAvgCoordinate(), brokerEntry.getValue().getBrokerNwCoordinate()) < topicEntry.getValue().distanceToBroker()) {
                        try {
                            selectedBundleCache.put(entry.getKey(), topicEntry.getKey());
                        }
                        catch (Exception e) {
                            log.warn("Cannot find bundle!: {}", e);
                        }
                    }
                }
            }
        }
        log.info("Load Shedding Completed");
        return selectedBundleCache;
    } 
}
