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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@Parameters(commandDescription = "Produce or consume messages on a specified topic")
public class CetusClientTestApp {

    @Parameter(names = { "--url" }, description = "Broker URL to which to connect.")
    String serviceURL = null;

    @Parameter(names = { "--auth-plugin" }, description = "Authentication plugin class name.")
    String authPluginClassName = null;

    @Parameter(names = { "--auth-params" }, description = "Authentication parameters, e.g., \"key1:val1,key2:val2\".")
    String authParams = null;

    @Parameter(names = { "-h", "--help", }, help = true, description = "Show this help.")
    boolean help;

    @Parameter(names = { "-rf", "--run-forever" }, description = "set this flag to run forever" )
    boolean runEternally;

    @Parameter(names = { "--inject-coordinates" }, description = "set this flag to inject coordinates" )
    boolean injectCoordinates;

    @Parameter(names = { "--use-nc-proxy" }, description = "set this flag to use NC proxy instead of agent running on client" )
    boolean useNcProxy;

    @Parameter(names = { "--disable-next-broker-hint" }, description = "set this flag to disable the use of next broker hint" )
    boolean disableNextBrokerHint;

    @Parameter(names = { "--send-coordinates-secs" }, description = "Period (in secs) to send coordinates to broker" )
    int sendCoordinateSecs = 5;

    @Parameter(names = { "--update-serf-gw-secs" }, description = "Period (in secs) to check for Serf GW update" )
    int updateSerfGwSecs = 5;

    @Parameter(names = { "-nt", "--num-topics" }, description = "Number of topics to create")
    int numTopics;

    @Parameter(names = { "-nc", "--num-clients" }, description = "Number of clients to create per topic (either producer or consumer)")
    int numClients;

    boolean tlsAllowInsecureConnection = false;
    boolean tlsEnableHostnameVerification = false;
    String tlsTrustCertsFilePath = null;

    JCommander commandParser;
    CmdProduceTopicGen produceCommand;
    CmdConsumeTopicGen consumeCommand;

    public CetusClientTestApp(Properties properties) throws MalformedURLException {
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
        this.injectCoordinates = Boolean.parseBoolean(properties.getProperty("injectCoordinates", "false"));
        this.useNcProxy = Boolean.parseBoolean(properties.getProperty("useNcProxy", "false"));
        this.tlsAllowInsecureConnection = Boolean
                .parseBoolean(properties.getProperty("tlsAllowInsecureConnection", "false"));
        this.tlsEnableHostnameVerification = Boolean
                .parseBoolean(properties.getProperty("tlsEnableHostnameVerification", "false"));
        this.tlsTrustCertsFilePath = properties.getProperty("tlsTrustCertsFilePath");

        produceCommand = new CmdProduceTopicGen();
        consumeCommand = new CmdConsumeTopicGen();

        this.commandParser = new JCommander();
        commandParser.setProgramName("pulsar-client");
        commandParser.addObject(this);
        commandParser.addCommand("produce", produceCommand);
        commandParser.addCommand("consume", consumeCommand);
    }

    private void updateConfig() throws UnsupportedAuthenticationException, MalformedURLException {
        ClientBuilder clientBuilder = PulsarClient.builder();
        if (isNotBlank(this.authPluginClassName)) {
            clientBuilder.authentication(authPluginClassName, authParams);
        }
        clientBuilder.allowTlsInsecureConnection(this.tlsAllowInsecureConnection);
        clientBuilder.tlsTrustCertsFilePath(this.tlsTrustCertsFilePath);
        clientBuilder.serviceUrl(serviceURL);
        clientBuilder.setUseSerfCoordinates(!injectCoordinates);
        clientBuilder.setUseNetworkCoordinateProxy(useNcProxy);
        clientBuilder.setEnableNextBrokerHint(!disableNextBrokerHint);
        clientBuilder.setUpdateSerfGwSecs(updateSerfGwSecs);
        clientBuilder.setSendCoordinateSecs(sendCoordinateSecs);
        this.produceCommand.updateConfig(clientBuilder);
        this.consumeCommand.updateConfig(clientBuilder);
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

            String chosenCommand = commandParser.getParsedCommand();
            if ("produce".equals(chosenCommand)) {
                if(runEternally == true) {
                    return produceCommand.runForever();
                }
                else { 
                    return produceCommand.run();
                }
            } else if ("consume".equals(chosenCommand)) {
                if(runEternally == true) {
                    return consumeCommand.runForever();
                }
                else {
                    return consumeCommand.run();
                }
            } else {
                commandParser.usage();
                return -1;
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
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: cetus-client CONF_FILE_PATH [options] [command] [command options]");
            System.exit(-1);
        }
        String configFile = args[0];
        Properties properties = new Properties();

        if (configFile != null) {
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(configFile);
                properties.load(fis);
            } finally {
                if (fis != null) {
                    fis.close();
                }
            }
        }

        CetusClientTestApp clientTool = new CetusClientTestApp(properties);
        int exit_code = clientTool.run(Arrays.copyOfRange(args, 1, args.length));

	    //Thread.sleep (10000);

        System.exit(exit_code);

    }
}
