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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.HexDump;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.Executors;

import com.google.common.collect.Lists;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
/**
 * pulsar-client consume command implementation.
 *
 */
@Parameters(commandDescription = "Consume messages from a specified topic")
public class DumbConsumerGen {

    private static final Logger LOG = LoggerFactory.getLogger(DumbConsumerGen.class);
    private static final String MESSAGE_BOUNDARY = "----- got message -----";

    @Parameter(description = "TopicName", required = true)
    private List<String> mainOptions = new ArrayList<String>();

    @Parameter(names = { "-t", "--subscription-type" }, description = "Subscription type: Exclusive, Shared, Failover.")
    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    @Parameter(names = { "-s", "--subscription-name" }, required = true, description = "Subscription name.")
    private String subscriptionName;

    @Parameter(names = { "-nt", "--num-topics" }, description = "How many topics and consumers to create/subscribe to ")
    private int numTopics;

    @Parameter(names = { "-ns", "--namespace" }, description = "Name of namespace (e.g. pulsar-cluster-1/cetus)")
    private String namespace = "pulsar-cluster-1/cetus";

    @Parameter(names = { "-tp", "--topic-prefix" }, description = "Topic prefix (e.g. my-topic)")
    private String topicPrefix = "my-topic";

    @Parameter(names = { "-si", "--inter-consumer-sleep-ms" }, description = "Milliseconds between creating 2 consumers")
    private int interConsumerSleepMs = 1000;

    @Parameter(names = { "-nc", "--num-clients" }, description = "Number of clients to create per topic (either producer or consumer)")
    private int numClients = 1;

    ClientBuilder clientBuilder;

    /**
     * Set client configuration.
     *
     */
    public void updateConfig(ClientBuilder clientBuilder) {
        this.clientBuilder = clientBuilder;
    }


    private String generateTopicName(int idx) {
        String topicName = String.format("non-persistent://public/%s/%s_%d", this.namespace, this.topicPrefix, idx);
        return topicName;
    }

    /**
     * Run the consume command.
     *
     * @return 0 for success, < 0 otherwise
     */
    public int run() throws PulsarClientException, IOException {
        if (this.subscriptionName == null || this.subscriptionName.isEmpty())
            throw (new ParameterException("Subscription name is not provided."));

        String[] topics = new String[this.numTopics];
        for(int i = 0; i < numTopics; i++) {
            topics[i] = String.format(generateTopicName(i));
            LOG.info("Topic: {}", topics[i]);
        }

        List<PulsarClient> clients = new ArrayList<>();
        List<Consumer<byte[]>> consumers = new ArrayList<>();
        for (String topic : topics) {
            for (int clientIdx = 0; clientIdx < numClients; clientIdx++) {
                try {
                    PulsarClient client = clientBuilder.build();
                    Consumer<byte[]> consumer = client.newConsumer().topic(topic).subscriptionName(topic).subscriptionType(subscriptionType).subscribe();
                    clients.add(client);
                    consumers.add(consumer);
                } catch (Exception e) {
                    LOG.error("Error while consuming messages");
                    LOG.error(e.getMessage(), e);
                } finally {
                    LOG.info("Consumer created for topic {} ", topic);
                }
                try {
                    Thread.sleep(interConsumerSleepMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        while (true) {
            try {
                Thread.sleep(1);
            } catch (Exception e) {
                LOG.error("Exception caught while sleeping after all consumers were created");
            }
        }
    }
}
