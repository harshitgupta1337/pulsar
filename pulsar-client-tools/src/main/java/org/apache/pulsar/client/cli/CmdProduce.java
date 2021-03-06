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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * pulsar-client produce command implementation.
 *
 */
@Parameters(commandDescription = "Produce messages to a specified topic")
public class CmdProduce {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarClientTool.class);
    private static final int MAX_MESSAGES = 1000;

    @Parameter(description = "TopicName", required = true)
    private List<String> mainOptions;

    @Parameter(names = { "-m", "--messages" }, description = "Comma separted string messages to send, "
            + "either -m or -f must be specified.")
    private List<String> messages = Lists.newArrayList();

    @Parameter(names = { "-f", "--files" }, description = "Comma separated file paths to send, either "
            + "-m or -f must be specified.")
    private List<String> messageFileNames = Lists.newArrayList();

    @Parameter(names = { "-n", "--num-produce" }, description = "Number of times to send message(s), "
            + "the count of messages/files * num-produce should below than " + MAX_MESSAGES + ".")
    private int numTimesProduce = 1;

    @Parameter(names = { "-r", "--rate" }, description = "Rate (in msg/sec) at which to produce, "
            + "value 0 means to produce messages as fast as possible.")
    private double publishRate = 0;

    boolean noMessages = false;

    ClientBuilder clientBuilder;

    public CmdProduce() {
        // Do nothing
    }

    /**
     * Set Pulsar client configuration.
     *
     */
    public void updateConfig(ClientBuilder newBuilder) {
        this.clientBuilder = newBuilder;
    }

    /*
     * Generate a list of message bodies which can be used to build messages
     *
     * @param stringMessages List of strings to send
     *
     * @param messageFileNames List of file names to read and send
     *
     * @return list of message bodies
     */
    private List<byte[]> generateMessageBodies(List<String> stringMessages, List<String> messageFileNames) {
        List<byte[]> messageBodies = new ArrayList<byte[]>();

        for (String m : stringMessages) {
            messageBodies.add(m.getBytes());
        }

        try {
            for (String filename : messageFileNames) {
                File f = new File(filename);
                FileInputStream fis = new FileInputStream(f);
                byte[] fileBytes = new byte[(int) f.length()];
                fis.read(fileBytes);
                messageBodies.add(fileBytes);
                fis.close();

            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        return messageBodies;
    }

    /**
     * Run the producer.
     *
     * @return 0 for success, < 0 otherwise
     * @throws Exception
     */
    public int run() throws PulsarClientException {
        if (mainOptions.size() != 1)
            throw (new ParameterException("Please provide one and only one topic name."));
        if (this.numTimesProduce <= 0)
            throw (new ParameterException("Number of times need to be positive number."));
        if (messages.size() == 0 && messageFileNames.size() == 0)
            throw (new ParameterException("Please supply message content with either --messages or --files"));

        int totalMessages = (messages.size() + messageFileNames.size()) * numTimesProduce;
        if (totalMessages > MAX_MESSAGES) {
            String msg = "Attempting to send " + totalMessages + " messages. Please do not send more than "
                    + MAX_MESSAGES + " messages";
            throw new ParameterException(msg);
        }

        String topic = this.mainOptions.get(0);
        int numMessagesSent = 0;
        int returnCode = 0;

        try {
            PulsarClient client = clientBuilder.build();
            Producer<byte[]> producer = client.newProducer().topic(topic).create();

            List<byte[]> messageBodies = generateMessageBodies(this.messages, this.messageFileNames);
            RateLimiter limiter = (this.publishRate > 0) ? RateLimiter.create(this.publishRate) : null;
            for (int i = 0; i < this.numTimesProduce; i++) {
                for (byte[] content : messageBodies) {
                    if (limiter != null) {
                        limiter.acquire();
                    }

                    producer.send(content);
                    numMessagesSent++;
                }
            }

	try {
	Thread.sleep(10000);
	} catch (Exception e) {
		LOG.error("Failed to sleep inside run()");
	}

            client.close();
        } catch (Exception e) {
            LOG.error("Error while producing messages");
            LOG.error(e.getMessage(), e);
            returnCode = -1;
        } finally {
            LOG.info("{} messages successfully produced", numMessagesSent);
        }

        return returnCode;
    }

    public int runForever() throws PulsarClientException {
        if (mainOptions.size() != 1)
            throw (new ParameterException("Please provide one and only one topic name."));
        if (this.numTimesProduce <= 0)
            throw (new ParameterException("Number of times need to be positive number."));
        if (messages.size() == 0 && messageFileNames.size() == 0)
        {
            this.noMessages = true;
        }
            //throw (new ParameterException("Please supply message content with either --messages or --files"));
        if(this.noMessages == true)
        {
            LOG.info("Running forever with no messages");
            try {
                String topic = this.mainOptions.get(0);
                PulsarClient client = clientBuilder.build();
                Producer<byte[]> producer = client.newProducer().topic(topic).create();
            }
            catch (Exception e) {
                
            }
            while(true);
        }
        int totalMessages = (messages.size() + messageFileNames.size()) * numTimesProduce;
        if (totalMessages > MAX_MESSAGES) {
            String msg = "Attempting to send " + totalMessages + " messages. Please do not send more than "
                    + MAX_MESSAGES + " messages";
            throw new ParameterException(msg);
        }

        String topic = this.mainOptions.get(0);
        int numMessagesSent = 0;
        int returnCode = 0;

        try {
            PulsarClient client = clientBuilder.build();
            Producer<byte[]> producer = client.newProducer().topic(topic).create();

            List<byte[]> messageBodies = generateMessageBodies(this.messages, this.messageFileNames);
            RateLimiter limiter = (this.publishRate > 0) ? RateLimiter.create(this.publishRate) : null;
            while(true) {
                for (int i = 0; i < this.numTimesProduce; i++) {
                    for (byte[] content : messageBodies) {
                        if (limiter != null) {
                            limiter.acquire();
                        }

                        producer.send(content);
                        numMessagesSent++;
                    }
                }
                try {
	                Thread.sleep(1000);
	            } 
                catch (Exception e) {
		           LOG.error("Failed to sleep inside run()");
	            }
            }



            //client.close();
        } catch (Exception e) {
            LOG.error("Error while producing messages");
            LOG.error(e.getMessage(), e);
            returnCode = -1;
        } finally {
            LOG.info("{} messages successfully produced", numMessagesSent);
        }

        return returnCode;
    }
 
}
