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
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.common.policies.data.NetworkCoordinate;
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
import org.apache.pulsar.broker.loadbalance.CetusModularLoadManager;
import org.apache.pulsar.broker.loadbalance.BrokerChange;
import org.apache.pulsar.broker.loadbalance.CetusLoadData;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;


/**
 * Load shedding strategy which will attempt to unload bundles from brokers who are actually
 * closer to other brokers, until we have a better bundle shedding strategy
 * 
 * 
 * 
 * 
 */
public class CetusLoadAwareShedder  implements CetusBundleUnloadingStrategy {

    private static final Logger log = LoggerFactory.getLogger(CetusLoadShedder.class);

    private final Multimap<String, BrokerChange> selectedBundleCache = ArrayListMultimap.create();

    public Map<String, String> getCurrentTopicPartitioning(CetusLoadData cetusLoadData) {
        Map<String, String> p = new HashMap<>();
        ConcurrentHashMap<String, CetusBrokerData> cetusBrokerDataMap = cetusLoadData.getCetusBrokerDataMap();
        for (String broker : cetusBrokerDataMap.keySet()) {
            CetusBrokerData brokerData = cetusBrokerDataMap.get(broker);
            for (String bundle : brokerData.getBundleNetworkCoordinates().keySet()) {
                p.put(bundle, broker);
            }
        }
        return p;
    }

    public Map<String, Double> getCurrentTopicRates(CetusLoadData cetusLoadData) {
        Map<String, Double> rates = new HashMap<>();
        LoadData loadData = cetusLoadData.getLoadData();
        for (String bundle : loadData.getBundleData().keySet()) {
            BundleData bundleData = loadData.getBundleData().get(bundle);
            TimeAverageMessageData m = bundleData.getShortTermData();
            double tput = m.getMsgThroughputIn() + m.getMsgThroughputOut();
            rates.put(bundle, tput);
        }
        return rates;
    }

    public Map<String, Double> getBrokerRates(Map<String, Double> topicRates, Map<String, String> partitioning) {
        Map<String, Double> brokerRates = new HashMap<>();
        for (String topic : partitioning.keySet()) {
            String broker = partitioning.get(topic);
            if (!brokerRates.containsKey(broker)) 
                brokerRates.put(broker, 0.0);
            brokerRates.put(broker, brokerRates.get(broker) + topicRates.get(topic));
        }
        return brokerRates;
    }

    public Map<String, String> replaceTopics(List<Pair<String, String>> violatedTopics, CetusLoadData cetusLoadData, Map<String, String> currPartitioning, String loadMgrAddress, Map<String, Double> topicRates) {
        Map<String, String> partitioningChange = new HashMap<>(); // to store the new broker of violated topic

        ConcurrentHashMap<String, CetusBrokerData> cetusBrokerDataMap = cetusLoadData.getCetusBrokerDataMap();
        Map<String, List<String>> feasibleSetMap = new HashMap<>();
        List<Pair<String, Integer>> feasibleSetSizes = new ArrayList<>();
        // Step 2 : For each violated topic, 
        for (Pair<String, String> topicPair : violatedTopics) {
            String bundle = topicPair.getKey();
            String currBroker = topicPair.getValue();
            NetworkCoordinate bundleCoord = cetusBrokerDataMap.get(currBroker).getBundleNetworkCoordinates().get(bundle).getProducerConsumerAvgCoordinate();

            List<String> feasibleBrokers = new ArrayList<String>();

            // Now find the set of feasible brokers
            for(Map.Entry<String, CetusBrokerData> brokerEntry : cetusBrokerDataMap.entrySet()) {
                if (brokerEntry.getKey().equals(currBroker))
                    continue;

                if (brokerEntry.getKey().equals(loadMgrAddress.split("//")[1]))
                    continue;

                double distToOtherBroker = CoordinateUtil.calculateDistance(bundleCoord, brokerEntry.getValue().getBrokerNwCoordinate());

                if (distToOtherBroker*1000.0 < CetusModularLoadManager.CETUS_LATENCY_BOUND_MS) {
                    feasibleBrokers.add(brokerEntry.getKey());
                }
            }
            feasibleSetMap.put(bundle, feasibleBrokers);
            feasibleSetSizes.add(new ImmutablePair<>(bundle, feasibleBrokers.size()));
        }

        // Sort the violated topics in increasing order of the feasible set size
        Collections.sort(feasibleSetSizes, new Comparator<Pair<String, Integer>>() {
            @Override
            public int compare(final Pair<String, Integer> o1, final Pair<String, Integer> o2) {
                return o1.getValue() - o2.getValue();
            }
        });

        Map<String, Double> brokerRates = getBrokerRates(topicRates, currPartitioning);
        // Adding brokers that dont have topics
        for (String broker : cetusBrokerDataMap.keySet()) {
            if (!brokerRates.containsKey(broker))
                brokerRates.put(broker, 0.0);
        }

        log.info("Gere");
        for (Pair<String, Integer> topicPair : feasibleSetSizes) {
            String topic = topicPair.getKey();
            Double topicRate = topicRates.get(topic);
            List<String> feasibleBrokers = feasibleSetMap.get(topic);
            List<Pair<String, Double>> feasibleBrokerRates = new ArrayList<>();
            for (String broker : feasibleBrokers) {
                feasibleBrokerRates.add(new ImmutablePair<>(broker, brokerRates.get(broker)));
            }

            // Now rank the brokers based on increasing order of current load
            Collections.sort(feasibleBrokerRates, new Comparator<Pair<String, Double>>() {
                    @Override
                    public int compare(final Pair<String, Double> o1, final Pair<String, Double> o2) {
                        if (o2.getValue() > o1.getValue()) 
                            return -1;
                        else if (o2.getValue() < o1.getValue()) 
                            return 1;
                        else
                            return 0;
                    }
                    });

            for (Pair<String, Double> p : feasibleBrokerRates) {
                String broker = p.getKey();
                Double currRate = p.getValue();
                log.info("Feasible broker {} with rate {}", broker, currRate);
                // TODO Just place it on the least loaded broker. No need to check if this will cause overload
                partitioningChange.put(topic, broker);

                String currBroker = currPartitioning.get(topic);

                // Updating the rates to account for this migration
                brokerRates.put(currBroker, brokerRates.get(currBroker) - topicRate);
                brokerRates.put(broker, currRate + topicRate);

                break;
            }
        }
        return partitioningChange;
     }

    public Multimap<String, BrokerChange> findBundlesForUnloading(CetusLoadData cetusLoadData, ServiceConfiguration conf, String loadMgrAddress) {
        ConcurrentHashMap<String, CetusBrokerData> cetusBrokerDataMap = cetusLoadData.getCetusBrokerDataMap();
        LoadData loadData = cetusLoadData.getLoadData();
        selectedBundleCache.clear();

        // Step 0 (a) : Get the current topic partitioning
        Map<String, String> partitioning = getCurrentTopicPartitioning(cetusLoadData);
        Map<String, String> origPartitioning = new HashMap<String, String>();
        for (String bundle : partitioning.keySet())
            origPartitioning.put(bundle, partitioning.get(bundle));

        // Step 0 (b) : get the current mapping of bundle rates
        Map<String, Double> topicRates = getCurrentTopicRates(cetusLoadData);

        // Step 1 : Get all violated topics. Pair of <topic, broker>
        List<Pair<String, String>> violatedTopics = new ArrayList<>(); 

        // Step 1(a) : Also get a map of broker ---> message throughput
        Map<String, Double> brokerTputs = new HashMap<String, Double>();

        log.info("Finding Bundles to Unload: Brokers: {} ", cetusBrokerDataMap.entrySet());
        for(Map.Entry<String, CetusBrokerData> entry : cetusBrokerDataMap.entrySet()) {
            String broker = entry.getKey();
            double totalTput = 0;
            for(Map.Entry<String, CetusNetworkCoordinateData> topicEntry : entry.getValue().getBundleNetworkCoordinates().entrySet()) {
                String bundle = topicEntry.getKey();
                double distToCurrBroker = topicEntry.getValue().distanceToBroker();
                if (distToCurrBroker*1000.0 > CetusModularLoadManager.CETUS_LATENCY_BOUND_MS) {
                    log.info("[Cetus Load Shedder] Latency bound violated for bundle {}. Dist to curr broker {} = {}", bundle, broker, distToCurrBroker);
                    violatedTopics.add(new ImmutablePair<>(bundle, broker));
                }

                BundleData bundleData = loadData.getBundleData().get(bundle);
                double tput = bundleData.getShortTermData().getMsgThroughputIn() + bundleData.getShortTermData().getMsgThroughputOut();
                totalTput += tput;
            }
            log.info("Setting tput for broker {} = {}", broker, totalTput);
            brokerTputs.put(broker, totalTput);
        }

        Map<String, String> partitioningChange = replaceTopics(violatedTopics, cetusLoadData, partitioning, loadMgrAddress, topicRates);
        for (String bundle : partitioningChange.keySet()) {
            String nextBroker = partitioningChange.get(bundle);
            partitioning.put(bundle, nextBroker);
        }

        // Once all changes have completed, populate the selected bundle cache
        for (String bundle : partitioning.keySet()) {
            String newBroker = partitioning.get(bundle);
            String oldBroker = origPartitioning.get(bundle);
            if (!newBroker.equals(oldBroker)) {
                try {
                    selectedBundleCache.put(oldBroker, new BrokerChange(bundle, newBroker));
                }
                catch (Exception e) {
                    log.warn("Cannot find bundle!: {}", e);
                }
            }
        }

        log.info("Load Shedding Completed");
        return selectedBundleCache;
    } 
}
