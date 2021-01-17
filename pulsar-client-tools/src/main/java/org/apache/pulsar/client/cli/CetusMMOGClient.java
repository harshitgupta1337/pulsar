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
import java.io.FileReader;
import java.io.BufferedReader;
import java.net.MalformedURLException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReadWriteLock;
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
public class CetusMMOGClient {
    String authPluginClassName = null;
    String authParams = null;
    private String namespace = "pulsar-cluster-1/cetus";

    @Parameter(names = { "--url" }, description = "Broker URL to which to connect.")
    String serviceURL = null;

    @Parameter(names = { "-h", "--help", }, help = true, description = "Show this help.")
    boolean help;

    @Parameter(names = {"-r", "--message-rate" }, description = "Msgs/sec published to avatar's topic")
    int messageRate;
    @Parameter(names = {"-s", "--message-size" }, description = "Msg size in bytes")
    int messageSize;
    @Parameter(names = {"-f", "--client-group-file" }, description = "File containing group membership info")
    String clientGroupFile;

    @Parameter(names = { "-C", "--client-id" }, description = "Client ID")
    int clientId = 0;

    @Parameter(names = { "--use-nc-proxy" }, description = "set this flag to use NC proxy instead of agent running on client" )
    boolean useNcProxy;

    @Parameter(names = { "--disable-next-broker-hint" }, description = "set this flag to disable the use of next broker hint" )
    boolean disableNextBrokerHint;

    boolean tlsAllowInsecureConnection = false;
    boolean tlsEnableHostnameVerification = false;
    String tlsTrustCertsFilePath = null;

    JCommander commandParser;

    ClientBuilder clientBuilder;

    private class GroupChangeInfo {
        public int respawnTime;
        public List<Integer> groupMembers;
        public GroupChangeInfo(int ts, List<Integer> members) {
            this.respawnTime = ts ;
            this.groupMembers = members;
        }
    }

    List<GroupChangeInfo> groupChanges;

    ReadWriteLock lock;
    List<Integer> currGroupMembers;

    public void populateGroupChanges() {
        // Read the file and populate
        BufferedReader r;
        try {
            r = new BufferedReader(new FileReader(this.clientGroupFile));
            String line = r.readLine();
            while (line != null) {
                String []splits = line.split("\\s+");
                assert (splits.length > 1);
                int ts = Integer.parseInt(splits[0]);
                List<Integer> groupMembers = new ArrayList<>();
                System.out.print(ts);
                for (int idx = 1; idx < splits.length; idx++)  {
                    groupMembers.add(Integer.parseInt(splits[idx]));
                    System.out.print("\t"+splits[idx]);
                }
                System.out.println();
                this.groupChanges.add(new GroupChangeInfo(ts, groupMembers));
                line = r.readLine();
            }
            for (int i = 0; i < this.groupChanges.size(); i++) {
                System.out.println("DDGC "+this.groupChanges.get(i).respawnTime+"\t"+this.groupChanges.get(i).groupMembers.size());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public CetusMMOGClient(Properties properties) throws MalformedURLException {
        this.serviceURL = StringUtils.isNotBlank(properties.getProperty("brokerServiceUrl"))
                ? properties.getProperty("brokerServiceUrl") : properties.getProperty("webServiceUrl");
        // fallback to previous-version serviceUrl property to maintain backward-compatibility
        if (StringUtils.isBlank(this.serviceURL)) {
            this.serviceURL = properties.getProperty("serviceUrl");
        }
        this.authPluginClassName = properties.getProperty("authPlugin");
        this.authParams = properties.getProperty("authParams");
        this.tlsAllowInsecureConnection = Boolean
                .parseBoolean(properties.getProperty("tlsAllowInsecureConnection", "false"));
        this.tlsEnableHostnameVerification = Boolean
                .parseBoolean(properties.getProperty("tlsEnableHostnameVerification", "false"));
        this.tlsTrustCertsFilePath = properties.getProperty("tlsTrustCertsFilePath");
        this.groupChanges = new ArrayList<GroupChangeInfo>();
        this.lock = new ReentrantReadWriteLock();
        this.currGroupMembers = null;

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
        this.clientBuilder.setEnableNextBrokerHint(!disableNextBrokerHint);
    }

    private String genPayload(int size) {
        String res = "";
        for (int i = 0; i < size; i++) 
            res += "0";
        return res;
    }

    private void produceMessages(String topic, int payloadSize, int produceRate) {
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
                String messagePayload = ts + " " + this.genPayload(payloadSize - ts.length());
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

    private void consumeMessages(String topic, Integer aoiAvatarId) {
        try{
            int numMessagesConsumed = 0;
            PulsarClient client = clientBuilder.build();
            Consumer<byte[]> consumer = client.newConsumer().topic(topic).subscriptionName(topic).subscriptionType(SubscriptionType.Shared).subscribe();

            int lastMsgId = -1;
            while (true) {
                Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
                boolean found = false;                
                this.lock.readLock().lock();
                for (Integer c : this.currGroupMembers) {
                    if (c == aoiAvatarId) {
                        found = true;
                        break;
                    }
                }
                this.lock.readLock().unlock();

                if (!found)
                    break;

                if (msg == null) {
                } else {
                    numMessagesConsumed += 1;
                    String output = this.interpretMessage(msg, false);
                    long ts = System.currentTimeMillis() ;
                    String splits[] = output.split(" ");
                    long sentTs = Long.parseLong(splits[0]);
                    System.out.println("E2E DELAY at "+sentTs+" = " + (ts - sentTs) + " ms");
                    consumer.acknowledge(msg);
                }
            }
            System.out.println("Closing client for aoi client "+aoiAvatarId);
            client.close();
        }
        catch (Exception e) {
        }
    }

    private String generateFullTopicName(String smallTopicName) {
        String topicName = String.format("non-persistent://public/%s/%s", this.namespace, smallTopicName);
        return topicName;
    }

    private void updateCurrGroupMembers(List<Integer> members) {
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

            populateGroupChanges();

            ScheduledExecutorService serviceProd = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("clients"));
            String ownerTopic = generateFullTopicName("avatar_"+clientId);
            serviceProd.schedule(safeRun(() -> produceMessages(ownerTopic, messageSize, messageRate)), 0, TimeUnit.MILLISECONDS);

            for (int i = 0; i < this.groupChanges.size(); i++) {
                System.out.println("GC "+this.groupChanges.get(i).respawnTime);
            }

            long initTs = System.currentTimeMillis();
            int idx = 0;
            while (idx < this.groupChanges.size()) {
                System.out.println("---------------------");
                GroupChangeInfo nextGroup = this.groupChanges.get(idx);
                int nextTs = nextGroup.respawnTime;
                long currTs = System.currentTimeMillis();
                double timePassed = ((currTs - initTs)/1000.0);
                int msToSleep = (nextTs*1000 - (int)(currTs - initTs));
                if (msToSleep > 0) {
                    try {
                        Thread.sleep(msToSleep);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                //  Change group membership
                this.lock.writeLock().lock();
                this.currGroupMembers = new ArrayList<>(nextGroup.groupMembers);
                this.lock.writeLock().unlock();
                Map<Integer, Boolean> prevMembers = new HashMap<>();
                if (idx > 0) {
                    for (int m : this.groupChanges.get(idx-1).groupMembers) {
                        prevMembers.put(m, true);
                    }
                }
                for (int m : this.currGroupMembers) {
                    if (prevMembers.get(m) == null && m != this.clientId ) { // not present in previous group
                        ScheduledExecutorService serviceCons = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("clients"));
                        String topic = generateFullTopicName("avatar_"+m);
                        System.out.println("Creating consumer for client id "+m);
                        serviceCons.schedule(safeRun(() -> consumeMessages(topic, m)), 0, TimeUnit.MILLISECONDS);
                    }
                }
                idx++;
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
                Thread.sleep(1000);
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

        CetusMMOGClient clientTool = new CetusMMOGClient(properties);
        int exit_code = clientTool.run(Arrays.copyOfRange(args, 1, args.length));

        System.exit(exit_code);

    }
}