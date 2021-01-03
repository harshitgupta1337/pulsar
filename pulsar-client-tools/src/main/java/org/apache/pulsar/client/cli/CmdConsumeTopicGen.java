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
public class CmdConsumeTopicGen {

    private static final Logger LOG = LoggerFactory.getLogger(CmdConsumeTopicGen.class);
    private static final String MESSAGE_BOUNDARY = "----- got message -----";

    @Parameter(description = "TopicName", required = true)
    private List<String> mainOptions = new ArrayList<String>();

    @Parameter(names = { "-t", "--subscription-type" }, description = "Subscription type: Exclusive, Shared, Failover.")
    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    @Parameter(names = { "-s", "--subscription-name" }, required = true, description = "Subscription name.")
    private String subscriptionName;

    @Parameter(names = { "-n",
            "--num-messages" }, description = "Number of messages to consume, 0 means to consume forever.")
    private int numMessagesToConsume = 1;

    @Parameter(names = { "--hex" }, description = "Display binary messages in hex.")
    private boolean displayHex = false;

    @Parameter(names = { "-r", "--rate" }, description = "Rate (in msg/sec) at which to consume, "
            + "value 0 means to consume messages as fast as possible.")
    private double consumeRate = 0;

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


    public CmdConsumeTopicGen() {
        // Do nothing
    }

    /**
     * Set client configuration.
     *
     */
    public void updateConfig(ClientBuilder clientBuilder) {
        this.clientBuilder = clientBuilder;
    }

    /**
     * Interprets the message to create a string representation
     *
     * @param message
     *            The message to interpret
     * @param displayHex
     *            Whether to display BytesMessages in hexdump style, ignored for simple text messages
     * @return String representation of the message
     */
    private String interpretMessage(Message<byte[]> message, boolean displayHex) throws IOException {
        byte[] msgData = message.getData();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (!displayHex) {
            return new String(msgData);
        } else {
            HexDump.dump(msgData, 0, out, 0);
            return new String(out.toByteArray());
        }
    }

    private void consume(String topic) {
     
        try{
            int numMessagesConsumed = 0;
            PulsarClient client = clientBuilder.build();
            // TODO Using the topic name as subscription name for now
            Consumer<byte[]> consumer = client.newConsumer().topic(topic).subscriptionName(topic).subscriptionType(subscriptionType).subscribe();

                int lastMsgId = -1;
                RateLimiter limiter = (this.consumeRate > 0) ? RateLimiter.create(this.consumeRate) : null;
                while (this.numMessagesToConsume == 0 || numMessagesConsumed < this.numMessagesToConsume)            {
                    if (limiter != null) {
                        limiter.acquire();
                    }

                    Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
                    if (msg == null) {
                        LOG.info("No message to consume after waiting for 5 seconds for topic {}.", topic);
                    } else {
                        numMessagesConsumed += 1;
                        String output = this.interpretMessage(msg, displayHex);
                        String[] splited = output.split("\\s+");

                        int msgId = Integer.parseInt(splited[0]);
                        if (lastMsgId == -1) {
                            
                        } else if (msgId != lastMsgId + 1) {
                            if (msgId == lastMsgId)
                                LOG.info("PUBSUB_MSGERROR Redundant message on topic {} with ID {}", topic, msgId);
                            else if (msgId < lastMsgId)
                                LOG.info("PUBSUB_MSGERROR Out-of-order delivery on topic {} with ID {}", topic, msgId);
                            else
                                LOG.info("PUBSUB_MSGDROP Dropped message on topic {} Expected ID {} Recvd ID {}", topic, (lastMsgId+1), msgId);
                        }
                        lastMsgId = msgId;

                        //System.out.println(output);
                        long currTime = System.currentTimeMillis();
                        long sentTime = Long.parseLong(splited[1]);
                        LOG.info("PUBSUB_DELAY for topic {} = {}", topic, currTime-sentTime);
                        consumer.acknowledge(msg);
                    }
                }
                LOG.info("Finished consuming messages");
                client.close();
        }
        catch (Exception e) {
            LOG.debug("Exception in consume");
        }
    }

    private String generateTopicName(int idx) {
        String topicName = String.format("persistent://public/%s/%s_%d", this.namespace, this.topicPrefix, idx);
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
        if (this.numMessagesToConsume < 0)
            throw (new ParameterException("Number of messages should be zero or positive."));

        int numMessagesConsumed = 0;
        int returnCode = 0;
        
        String[] topics = new String[this.numTopics];
        for(int i = 0; i < numTopics; i++) {
            topics[i] = String.format(generateTopicName(i));
            LOG.info("Topic: {}", topics[i]);
        }

        for (String topic : topics) {
            for (int clientIdx = 0; clientIdx < numClients; clientIdx++) {
                try {
                    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("consumers"));
                    service.schedule(safeRun(() -> consume(topic)), 0, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    LOG.error("Error while consuming messages");
                    LOG.error(e.getMessage(), e);
                    returnCode = -1;
                } finally {
                    LOG.info("Consumer XYZZY created for topic {} ", topic);
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

    public int runForever() throws PulsarClientException, IOException {
        if (this.subscriptionName == null || this.subscriptionName.isEmpty())
            throw (new ParameterException("Subscription name is not provided."));
        if (this.numMessagesToConsume < 0)
            throw (new ParameterException("Number of messages should be zero or positive."));

        String topic = this.mainOptions.get(0);
        int numMessagesConsumed = 0;
        int returnCode = 0;

        try {
            PulsarClient client = clientBuilder.build();
            Consumer<byte[]> consumer = client.newConsumer().topic(topic).subscriptionName(this.subscriptionName).subscriptionType(subscriptionType).subscribe();

            RateLimiter limiter = (this.consumeRate > 0) ? RateLimiter.create(this.consumeRate) : null;
            while (true) {
                if (limiter != null) {
                    limiter.acquire();
                }

                Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
                if (msg == null) {
                    LOG.debug("No message to consume after waiting for 5 seconds.");
                } else {
                    numMessagesConsumed += 1;
                    System.out.println(MESSAGE_BOUNDARY);
                    String output = this.interpretMessage(msg, displayHex);
                    System.out.println(output);
                    consumer.acknowledge(msg);
                }
            }
            //client.close();
        } catch (Exception e) {
            LOG.error("Error while consuming messages");
            LOG.error(e.getMessage(), e);
            returnCode = -1;
        } finally {
            LOG.info("{} messages successfully consumed", numMessagesConsumed);
        }

        return returnCode;
    }
}
