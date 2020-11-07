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
package org.apache.pulsar.policies.data.loadbalancer;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.PulsarHandler;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetNetworkCoordinate;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetNetworkCoordinateResponse;
import org.apache.pulsar.common.policies.data.NetworkCoordinate;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.common.util.CoordinateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.pulsar.policies.data.loadbalancer.JSONWritable;

public class CetusNetworkCoordinateData {
    public static final Logger log = LoggerFactory.getLogger(CetusNetworkCoordinateData.class);

    //private CetusBrokerData cetusBrokerData;

    private NetworkCoordinate brokerCoordinate;

    private ConcurrentHashMap<String, NetworkCoordinate> producerCoordinates;
    private ConcurrentHashMap<String, NetworkCoordinate> consumerCoordinates;

    public CetusNetworkCoordinateData() {
        this.brokerCoordinate = new NetworkCoordinate();
        this.producerCoordinates = new ConcurrentHashMap<String, NetworkCoordinate>(16,1);
        this.consumerCoordinates = new ConcurrentHashMap<String, NetworkCoordinate>(16,1);
    } 

    public CetusNetworkCoordinateData(NetworkCoordinate brokerCoordinate) {
        this.brokerCoordinate = brokerCoordinate;
        this.producerCoordinates = new ConcurrentHashMap<String, NetworkCoordinate>(16,1);
        this.consumerCoordinates = new ConcurrentHashMap<String, NetworkCoordinate>(16,1);
    }

    public void putConsumerCoordinate(String nodeName, NetworkCoordinate coordinate) { 
        consumerCoordinates.put(nodeName, coordinate);
    }

    public void putProducerCoordinate(String nodeName, NetworkCoordinate coordinate) {
        producerCoordinates.put(nodeName, coordinate);
    }

    public NetworkCoordinate getConsumerCoordinate(String nodeName) {
        return consumerCoordinates.get(nodeName);
    }

    public NetworkCoordinate getProducerCoordinate(String nodeName) {
        return producerCoordinates.get(nodeName);
    }

    public void setBrokerCoordinate(NetworkCoordinate coordinate) {
        this.brokerCoordinate = coordinate;
    }

    public NetworkCoordinate getBrokerCoordinate() {
        return brokerCoordinate;
    }

    
    public double getProducerConsumerCoordinateAvgValue() {
        double total = 0.0;
        //producerCoordinates.forEach((producerId, coordinate) -> {
        for(Map.Entry<String, NetworkCoordinate> entry : producerCoordinates.entrySet()) {
            total += entry.getValue().getCoordinateAvg();
        }
        //consumerCoordinates.forEach((consumerId, coordinate) -> {
        for(Map.Entry<String, NetworkCoordinate> entry : consumerCoordinates.entrySet()) {
            total += entry.getValue().getCoordinateAvg();
        }

        return (double) (total/(producerCoordinates.size() + consumerCoordinates.size()));
    }

    public NetworkCoordinate getProducerConsumerAvgCoordinate() {
        double[] totalCoordinateVector = new double[8];
        double totalAdjustment = 0.0;
        double totalHeight = 0.0;
        double totalError = 0.0;

        if(producerCoordinates.size() == 0 && consumerCoordinates.size() == 0)
        {
            double[] coordinateVector = new double[]{0,0,0,0,0,0,0,0};
            NetworkCoordinate coordinate = new NetworkCoordinate(false, totalAdjustment, totalHeight, totalError, coordinateVector);
            return coordinate;
        }
        //producerCoordinates.forEach((producerId, coordinate) -> {
        for(Map.Entry<String, NetworkCoordinate> entry : producerCoordinates.entrySet()) {
            double[] coordinateVector = entry.getValue().getCoordinateVector();
            for(int i = 0; i < coordinateVector.length; i++) {
                totalCoordinateVector[i]+= coordinateVector[i];
            }
            totalAdjustment += entry.getValue().getAdjustment();
            totalHeight += entry.getValue().getHeight();
            totalError += entry.getValue().getError();
        }
        //consumerCoordinates.forEach((consumerId, coordinate) -> {
        int notValid = 0;
        log.info("Producers: {}, Consumers: {}", producerCoordinates.size(), consumerCoordinates.size());
        for(Map.Entry<String, NetworkCoordinate> entry : consumerCoordinates.entrySet()) {
            if(entry.getValue().isValid()) {
                log.info("Consumer Value is valid");
                double[] coordinateVector = entry.getValue().getCoordinateVector();
                for(int i = 0; i < coordinateVector.length; i++) {
                    totalCoordinateVector[i]+= coordinateVector[i];
                }
                totalAdjustment += entry.getValue().getAdjustment();
                totalHeight += entry.getValue().getHeight();
                totalError += entry.getValue().getError();
            }
            else {
                log.info("Consumer value is INVALID");
                notValid += 1;
            }
        }

        double[] avgCoordinateVector = new double[8];
        for(int i = 0; i < avgCoordinateVector.length; i++) {
            avgCoordinateVector[i] = (totalCoordinateVector[i]/(producerCoordinates.size() + consumerCoordinates.size() + notValid));
        }
        double avgAdjustment =  (totalAdjustment/(producerCoordinates.size() + consumerCoordinates.size() - notValid));
        double avgHeight =  (totalHeight/(producerCoordinates.size() + consumerCoordinates.size() + notValid));
        double avgError =   (totalError/(producerCoordinates.size() + consumerCoordinates.size() + notValid));
        NetworkCoordinate avgCoordinate = new NetworkCoordinate(true, avgAdjustment, avgError, avgHeight, avgCoordinateVector);

        log.info("Avg Coordinate: {} Adjustment {} Height {} Error {}", avgCoordinateVector, avgAdjustment, avgHeight, avgError);

        return avgCoordinate; 
    }


    public ConcurrentHashMap<String, NetworkCoordinate> getProducerCoordinates() {
        return producerCoordinates;
    }
    
    public void setProducerCoordinates(ConcurrentHashMap<String, NetworkCoordinate> producerCoordinates) {
        this.producerCoordinates = producerCoordinates;
    }

    public void setConsumerCoordinates(ConcurrentHashMap<String, NetworkCoordinate> consumerCoordinates) {
        this.consumerCoordinates = consumerCoordinates;
    }

    public ConcurrentHashMap<String, NetworkCoordinate> getConsumerCoordinates() { 
        return consumerCoordinates;
    }
    
    public double distanceToBroker() {
      return CoordinateUtil.calculateDistance(this.brokerCoordinate, getProducerConsumerAvgCoordinate());
    }
}
