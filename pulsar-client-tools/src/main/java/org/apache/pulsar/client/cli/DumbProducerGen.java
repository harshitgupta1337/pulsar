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
package org.apache.pulsar.client.cli;

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;

import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * pulsar-client produce command implementation.
 *
 */
@Parameters(commandDescription = "Produce messages to a specified number of topics")
public class DumbProducerGen {

    private static final Logger LOG = LoggerFactory.getLogger(CetusClientTestApp.class);

    @Parameter(description = "TopicName", required = true)
    private List<String> mainOptions;

    @Parameter(names = { "-nt", "--num-topics" }, description = "How many topics and producers to create")
    private int numTopics = 1;

    @Parameter(names = { "-ns", "--namespace" }, description = "Name of namespace (e.g. pulsar-cluster-1/cetus)")
    private String namespace = "pulsar-cluster-1/cetus";
    
    @Parameter(names = { "-tp", "--topic-prefix" }, description = "Topic prefix (e.g. my-topic)")
    private String topicPrefix = "my-topic";

    @Parameter(names = { "-si", "--inter-producer-sleep-ms" }, description = "Milliseconds between creating 2 producers")
    private int interProducerSleepMs = 1000;

    @Parameter(names = { "-nc", "--num-clients" }, description = "Number of clients to create per topic (either producer or consumer)")
    private int numClients = 1;
    
    boolean noMessages = false;

    ClientBuilder clientBuilder;
    
    /**
     * Set Pulsar client configuration.
     *
     */
    public void updateConfig(ClientBuilder newBuilder) {
        this.clientBuilder = newBuilder;
    }

    private String generateTopicName(int idx) {
        String topicName = String.format("non-persistent://public/%s/%s_%d", this.namespace, this.topicPrefix, idx);
        return topicName;
    }
    
    public int run() throws PulsarClientException {
        String[] topics = new String[this.numTopics];
        for(int i = 0; i < numTopics; i++)
        {
            topics[i] = String.format(generateTopicName(i));
            LOG.info("Topic: {}", topics[i]);
        }

        int numMessagesSent = 0;
        int returnCode = 0;

        List<PulsarClient> clients = new ArrayList<>();
        List<Producer<byte[]>> producers = new ArrayList<>();
        for(String topic : topics) {
            for (int clientIdx = 0; clientIdx < numClients; clientIdx++) {
                try {
                    PulsarClient client = clientBuilder.build();
                    Producer<byte[]> producer = client.newProducer().topic(topic).create();
                    clients.add(client);
                    producers.add(producer);
                } catch (Exception e) {
                    LOG.error("Error while producing messages");
                    LOG.error(e.getMessage(), e);
                    returnCode = -1;
                } finally {
                    LOG.info("{} messages successfully produced", numMessagesSent);
                }
                try {
                    Thread.sleep(interProducerSleepMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        while(true) {
            try {
                Thread.sleep(1);
            } catch (Exception e) {
                LOG.error("Error while sleeping after all producer threads have been created");
            }
        }
    }
}
