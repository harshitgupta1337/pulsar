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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.FileInputStream;
import java.io.ByteArrayOutputStream;
import java.net.MalformedURLException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;

import org.apache.commons.io.HexDump;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.util.concurrent.RateLimiter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import com.google.common.collect.Lists;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;

@Parameters(commandDescription = "Create a client for Cetus Drone swarm use-case")
public class CetusDroneClient {
    String authPluginClassName = null;
    String authParams = null;
    private String namespace = "pulsar-cluster-1/cetus";

    @Parameter(names = { "--url" }, description = "Broker URL to which to connect.")
    String serviceURL = null;

    @Parameter(names = { "-h", "--help", }, help = true, description = "Show this help.")
    boolean help;

    @Parameter(names = { "-rf", "--run-forever" }, description = "set this flag to run forever" )
    boolean runEternally;

    @Parameter(names = {"-fr", "--follow-rate" }, description = "Msgs/sec published to follow leader topic")
    int followRate;

    @Parameter(names = {"-dr", "--detection-rate" }, description = "Msgs/sec published to detection topic")
    int detectionRate;

    @Parameter(names = { "-nt", "--num-topics" }, description = "Number of topics to create")
    int numTopics;

    @Parameter(names = { "-L", "--is-leader" }, description = "Is leader of swarm")
    boolean isLeader;

    @Parameter(names = { "-S", "--swarm-id" }, description = "Swarm ID")
    int swarmId = 0;

    @Parameter(names = { "--use-nc-proxy" }, description = "set this flag to use NC proxy instead of agent running on client" )
    boolean useNcProxy;

    boolean tlsAllowInsecureConnection = false;
    boolean tlsEnableHostnameVerification = false;
    String tlsTrustCertsFilePath = null;

    JCommander commandParser;

    ClientBuilder clientBuilder;

    public CetusDroneClient(Properties properties) throws MalformedURLException {
        this.serviceURL = StringUtils.isNotBlank(properties.getProperty("brokerServiceUrl"))
                ? properties.getProperty("brokerServiceUrl") : properties.getProperty("webServiceUrl");
        // fallback to previous-version serviceUrl property to maintain backward-compatibility
        if (StringUtils.isBlank(this.serviceURL)) {
            this.serviceURL = properties.getProperty("serviceUrl");
        }
        this.authPluginClassName = properties.getProperty("authPlugin");
        this.authParams = properties.getProperty("authParams");
        this.numTopics = Integer.parseInt(properties.getProperty("numTopics", "1"));
        this.runEternally = Boolean.parseBoolean(properties.getProperty("runEternally", "false"));
        this.tlsAllowInsecureConnection = Boolean
                .parseBoolean(properties.getProperty("tlsAllowInsecureConnection", "false"));
        this.tlsEnableHostnameVerification = Boolean
                .parseBoolean(properties.getProperty("tlsEnableHostnameVerification", "false"));
        this.tlsTrustCertsFilePath = properties.getProperty("tlsTrustCertsFilePath");

        this.commandParser = new JCommander();
        commandParser.setProgramName("pulsar-client");
        commandParser.addObject(this);
    }

    private void updateConfig() throws UnsupportedAuthenticationException, MalformedURLException {
        this.clientBuilder = PulsarClient.builder();
        if (isNotBlank(this.authPluginClassName)) {
            this.clientBuilder.authentication(authPluginClassName, authParams);
        }
        this.clientBuilder.allowTlsInsecureConnection(this.tlsAllowInsecureConnection);
        this.clientBuilder.tlsTrustCertsFilePath(this.tlsTrustCertsFilePath);
        this.clientBuilder.serviceUrl(serviceURL);
        this.clientBuilder.setUseSerfCoordinates(true);
        this.clientBuilder.setUseNetworkCoordinateProxy(useNcProxy);
    }

    private void produceMessages(String topic, String messagePayload, int produceRate) {
        int numMessagesSent = 0;
        try {
            PulsarClient client = clientBuilder.build();
            Producer<byte[]> producer = client.newProducer().topic(topic).create();
            System.out.println("Created new producer for topic "+topic);

            RateLimiter limiter = (produceRate > 0) ? RateLimiter.create(produceRate) : null;
            while(true) {
                if (limiter != null) {
                    limiter.acquire();
                }

                String ts = Long.toString(System.currentTimeMillis());
                producer.send(messagePayload.getBytes());

                numMessagesSent++;
            }
        }
        catch (Exception e) {
        }
    }

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

    private void consumeMessages(String topic) {
        try{
            int numMessagesConsumed = 0;
            PulsarClient client = clientBuilder.build();
            Consumer<byte[]> consumer = client.newConsumer().topic(topic).subscriptionName(topic).subscriptionType(SubscriptionType.Shared).subscribe();

            int lastMsgId = -1;
            while (true) {
                Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
                if (msg == null) {
                } else {
                    numMessagesConsumed += 1;
                    String output = this.interpretMessage(msg, false);
                    System.out.println("Reccvd message "+output);
                    consumer.acknowledge(msg);
                }
            }
            //client.close();
        }
        catch (Exception e) {
        }
    }

    private String generateFullTopicName(String smallTopicName) {
        String topicName = String.format("non-persistent://public/%s/%s", this.namespace, smallTopicName);
        return topicName;
    }

    public int run(String[] args) {
        try {
            commandParser.parse(args);

            if (isBlank(this.serviceURL)) {
                commandParser.usage();
                return -1;
            }

            if (help) {
                commandParser.usage();
                return 0;
            }

            try {
                this.updateConfig(); // If the --url, --auth-plugin, or --auth-params parameter are not specified,
                // it will default to the values passed in by the constructor
            } catch (MalformedURLException mue) {
                System.out.println("Unable to parse URL " + this.serviceURL);
                commandParser.usage();
                return -1;
            } catch (UnsupportedAuthenticationException exp) {
                System.out.println("Failed to load an authentication plugin");
                commandParser.usage();
                return -1;
            }

            ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("clients"));

            String detectionTopic = generateFullTopicName("detections_"+swarmId);
            if (isLeader) {
                service.schedule(safeRun(() -> consumeMessages(detectionTopic)), 0, TimeUnit.MILLISECONDS);
            } else {
                service.schedule(safeRun(() -> produceMessages(detectionTopic, "DETECTION", detectionRate)), 0, TimeUnit.MILLISECONDS);
            }

            try {
                System.out.println("Sleeping here for 5 secs");
                Thread.sleep(5);
            } catch (Exception e) {
                System.err.println("Error while sleeping after all producer threads have been created");
            }
            
            ScheduledExecutorService service2 = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("clients"));

            String followLeaderTopic = generateFullTopicName("follow_leader_"+swarmId);
            if (isLeader) {
                service2.schedule(safeRun(() -> produceMessages(followLeaderTopic, "FOLLOW_LEADER", followRate)), 0, TimeUnit.MILLISECONDS);
            } else {
                service2.schedule(safeRun(() -> consumeMessages(followLeaderTopic)), 0, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            if (e instanceof ParameterException) {
                commandParser.usage();
            } else {
                e.printStackTrace();
            }
            return -1;
        }

        while(true) {
            try {
                Thread.sleep(1);
            } catch (Exception e) {
                System.err.println("Error while sleeping after all producer threads have been created");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: cetus-client CONF_FILE_PATH [options] [command] [command options]");
            System.exit(-1);
        }
        Properties properties = new Properties();

        CetusDroneClient clientTool = new CetusDroneClient(properties);
        int exit_code = clientTool.run(Arrays.copyOfRange(args, 1, args.length));

        System.exit(exit_code);

    }
}
