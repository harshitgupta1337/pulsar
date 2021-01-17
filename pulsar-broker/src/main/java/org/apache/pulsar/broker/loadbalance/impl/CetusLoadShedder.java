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
import org.apache.pulsar.policies.data.loadbalancer.CetusBundleCentroidMonitoringData;
import org.apache.pulsar.common.policies.data.NetworkCoordinate;
import org.apache.pulsar.broker.loadbalance.CetusModularLoadManager;
import org.apache.pulsar.broker.loadbalance.BrokerChange;
import org.apache.pulsar.broker.loadbalance.CetusLoadData;
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

  private final Multimap<String, BrokerChange> selectedBundleCache = ArrayListMultimap.create();

  public Multimap<String, BrokerChange> findBundlesForUnloading(CetusLoadData cetusLoadData, ServiceConfiguration conf, String loadMgrAddress, String brokerSelStrategy) {
      if (brokerSelStrategy.equals("CentroidMin"))
          return processCentroids(cetusLoadData.getCetusBrokerLatencyDataMap(), conf, loadMgrAddress);
      else if (brokerSelStrategy.equals("CentroidSat"))
          return processCentroids(cetusLoadData.getCetusBrokerLatencyDataMap(), conf, loadMgrAddress);
      else if (brokerSelStrategy.equals("AllPairsMin"))
          return processAllPairs(cetusLoadData.getCetusBrokerLatencyDataMap(), conf, loadMgrAddress);
      else
          throw new RuntimeException("Unsupported broker selection strategy "+brokerSelStrategy);
  }

  public Multimap<String, BrokerChange> processAllPairs(ConcurrentHashMap<String, CetusLatencyMonitoringData> brokerLatencyDataMap, ServiceConfiguration conf, String loadMgrAddress) {
      selectedBundleCache.clear();
      for(Map.Entry<String, CetusLatencyMonitoringData> entry : brokerLatencyDataMap.entrySet()) {
          for(Map.Entry<String, CetusNetworkCoordinateData> topicEntry: entry.getValue().getBundleNetworkCoordinates().entrySet()) {
              double distToCurrBroker = topicEntry.getValue().distanceToBroker();
              if (distToCurrBroker*1000.0 > CetusModularLoadManager.CETUS_LATENCY_BOUND_MS) {
                  log.info("[Cetus Load Shedder] Latency bound violated for bundle {}. Dist to curr broker {} = {}", topicEntry.getKey(), entry.getKey(), distToCurrBroker);
              }

              boolean betterBrokerFound = false;
              String betterBroker = null;
              double max_broker_score = 0;

              double total = 0;
              double count = 0;
              for(Map.Entry<String, NetworkCoordinate> producerCoordinate : topicEntry.getValue().getProducerCoordinates().entrySet()) {
                  for(Map.Entry<String, NetworkCoordinate> consumerCoordinate : topicEntry.getValue().getConsumerCoordinates().entrySet()) {
                      total += CoordinateUtil.calculateDistance(producerCoordinate.getValue(), entry.getValue().getBrokerNwCoordinate()) + CoordinateUtil.calculateDistance(consumerCoordinate.getValue(), entry.getValue().getBrokerNwCoordinate());
                      count += 1;
                  }
              }

              max_broker_score = total/count;

              for(Map.Entry<String, CetusLatencyMonitoringData> brokerEntry : brokerLatencyDataMap.entrySet()) {
                  if (brokerEntry.getKey().equals(entry.getKey())) 
                      continue;

                  if (brokerEntry.getKey().equals(loadMgrAddress.split("//")[1]))
                      continue;

                  double broker_score = 0;
                  total = 0;
                  count = 0;

                  for(Map.Entry<String, NetworkCoordinate> producerCoordinate : topicEntry.getValue().getProducerCoordinates().entrySet()) {
                      for(Map.Entry<String, NetworkCoordinate> consumerCoordinate : topicEntry.getValue().getConsumerCoordinates().entrySet()) {
                          total += CoordinateUtil.calculateDistance(producerCoordinate.getValue(), brokerEntry.getValue().getBrokerNwCoordinate()) + CoordinateUtil.calculateDistance(consumerCoordinate.getValue(), brokerEntry.getValue().getBrokerNwCoordinate());
                          count += 1;
                      }
                  }

                  broker_score = total/count;
                  if(broker_score > max_broker_score) {
                      betterBrokerFound = true;
                      betterBroker = brokerEntry.getKey();
                      max_broker_score = broker_score;
                  }
              }          
              if (betterBrokerFound && betterBroker != null) {
                  try {
                      selectedBundleCache.put(entry.getKey(), new BrokerChange(topicEntry.getKey(), betterBroker));
                  }
                  catch (Exception e) {
                      log.warn("Cannot find bundle!: {}", e);
                  } 
              } else {
                  log.warn("[Cetus Load Shedder] Bundle {} is violated. But no other broker available. Sticking to bad broker", topicEntry.getKey());
              }
          }
      }

      log.info("Load Shedding Completed");
      return selectedBundleCache;
  }
 
  private boolean isTopicViolated(CetusBundleCentroidMonitoringData centroidData) {
    if (centroidData.worstCaseLatency*1000 > CetusModularLoadManager.CETUS_LATENCY_BOUND_MS)
        return true;
    else
        return false;
  }

  private double computeApproxE2eLatency(NetworkCoordinate brokerNC, CetusBundleCentroidMonitoringData centroidData) {
    double prodDist = CoordinateUtil.calculateDistance(brokerNC, centroidData.producerCentroid);
    double consDist = CoordinateUtil.calculateDistance(brokerNC, centroidData.consumerCentroid);
    return prodDist+consDist+centroidData.centroidDevn;
  }
 
  public Multimap<String, BrokerChange> processCentroids(ConcurrentHashMap<String, CetusLatencyMonitoringData> brokerLatencyDataMap, ServiceConfiguration conf, String loadMgrAddress) {
      if(conf.getCetusBrokerSelectionStrategy().equals("CentroidSat")) {
          selectedBundleCache.clear();
          log.info("LOAD MANAGER ADDR : {}", loadMgrAddress.split("//")[1]);
          log.info("SelectedBundleCache: {}", selectedBundleCache.toString());
          log.info("Finding Bundles to Unload: Brokers: {} ", brokerLatencyDataMap.entrySet());
          for(Map.Entry<String, CetusLatencyMonitoringData> entry : brokerLatencyDataMap.entrySet()) {
              for(Map.Entry<String, CetusBundleCentroidMonitoringData> topicEntry : entry.getValue().getBundleCentroidData().entrySet()) {
                  boolean violated = this.isTopicViolated(topicEntry.getValue());

                  if (violated) {
                      log.info("[Cetus Load Shedder] Latency bound violated for bundle {}. curr broker {}", topicEntry.getKey(), entry.getKey());

                      boolean betterBrokerFound = false;
                      String betterBroker = null;
                      for(Map.Entry<String, CetusLatencyMonitoringData> brokerEntry : brokerLatencyDataMap.entrySet()) {
                          if (brokerEntry.getKey().equals(entry.getKey())) 
                              continue;

                          if (brokerEntry.getKey().equals(loadMgrAddress.split("//")[1]))
                              continue;

                          double distToOtherBroker = computeApproxE2eLatency(brokerEntry.getValue().getBrokerNwCoordinate(), topicEntry.getValue());
                          log.info("[Cetus Load Shedder] Distance of bundle {} to broker {} = {}, current latency bound = {}", topicEntry.getKey(), brokerEntry.getKey(), distToOtherBroker*1000.0, CetusModularLoadManager.CETUS_LATENCY_BOUND_MS);
                          if (distToOtherBroker*1000.0 < CetusModularLoadManager.CETUS_LATENCY_BOUND_MS) {
                              betterBrokerFound = true;
                              betterBroker = brokerEntry.getKey();
                              break;
                          }
                      }
                      if (betterBrokerFound && betterBroker != null) {
                          try {
                              selectedBundleCache.put(entry.getKey(), new BrokerChange(topicEntry.getKey(), betterBroker));
                          }
                          catch (Exception e) {
                              log.warn("Cannot find bundle!: {}", e);
                          } 
                      } else {
                          log.warn("[Cetus Load Shedder] Bundle {} is violated. But no other broker available. Sticking to bad broker", topicEntry.getKey());
                      }
                  }
              }
          }
      } else if(conf.getCetusBrokerSelectionStrategy().equals("CentroidMin")) {
          selectedBundleCache.clear();
          log.info("LOAD MANAGER ADDR : {}", loadMgrAddress.split("//")[1]);
          log.info("SelectedBundleCache: {}", selectedBundleCache.toString());
          log.info("Finding Bundles to Unload: Brokers: {} ", brokerLatencyDataMap.entrySet());
          for(Map.Entry<String, CetusLatencyMonitoringData> entry : brokerLatencyDataMap.entrySet()) {
              for(Map.Entry<String, CetusBundleCentroidMonitoringData> topicEntry : entry.getValue().getBundleCentroidData().entrySet()) {
                  boolean violated = this.isTopicViolated(topicEntry.getValue());
                  if (violated) {
                      log.info("[Cetus Load Shedder] Latency bound violated for bundle {}. curr broker {}", topicEntry.getKey(), entry.getKey());

                      boolean betterBrokerFound = false;
                      double minBrokerLatency = CetusModularLoadManager.CETUS_LATENCY_BOUND_MS;
                      String betterBroker = null;
                      for(Map.Entry<String, CetusLatencyMonitoringData> brokerEntry : brokerLatencyDataMap.entrySet()) {
                          if (brokerEntry.getKey().equals(entry.getKey())) 
                              continue;

                          if (brokerEntry.getKey().equals(loadMgrAddress.split("//")[1]))
                              continue;

                          double distToOtherBroker = computeApproxE2eLatency(brokerEntry.getValue().getBrokerNwCoordinate(), topicEntry.getValue());
                          log.info("[Cetus Load Shedder] Distance of bundle {} to broker {} = {}, current latency bound = {}", topicEntry.getKey(), brokerEntry.getKey(), distToOtherBroker*1000.0, CetusModularLoadManager.CETUS_LATENCY_BOUND_MS);
                          if (distToOtherBroker*1000.0 < minBrokerLatency) {
                              betterBrokerFound = true;
                              minBrokerLatency = distToOtherBroker*1000;
                              betterBroker = brokerEntry.getKey();
                          }
                      }
                      if (betterBrokerFound && betterBroker != null) {
                          try {
                              selectedBundleCache.put(entry.getKey(), new BrokerChange(topicEntry.getKey(), betterBroker));
                          }
                          catch (Exception e) {
                              log.warn("Cannot find bundle!: {}", e);
                          } 
                      } else {
                          log.warn("[Cetus Load Shedder] Bundle {} is violated. But no other broker available. Sticking to bad broker", topicEntry.getKey());
                      }
                  }
              }
          }
      } 
      log.info("Load Shedding Completed");
      return selectedBundleCache;
  } 
}
