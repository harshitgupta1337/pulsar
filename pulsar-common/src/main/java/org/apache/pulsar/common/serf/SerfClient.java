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
package org.apache.pulsar.common.serf;

import static com.google.common.base.Preconditions.checkArgument;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.resumeChecksum;
import static java.lang.String.format;
import static org.apache.pulsar.common.api.Commands.hasChecksum;
import static org.apache.pulsar.common.api.Commands.readChecksum;
// CETUS
//import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import io.netty.util.concurrent.DefaultThreadFactory;
//***********************************************************************
import com.google.common.collect.Queues;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.ScheduledFuture;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import com.google.common.collect.ImmutableList;

// Serf Client includes
import no.tv2.serf.client.*;
import org.apache.pulsar.common.policies.data.NetworkCoordinate;

public class SerfClient {

    public static final Logger log= LoggerFactory.getLogger(SerfClient.class);

    private SerfEndpoint ep;
    private Client client;
    private final String nodeName;
    //private Query query;

    public SerfClient(String ip, long port, String nodeName)  {
        this.nodeName = nodeName;
        try {
            ep = new SocketEndpoint(ip, (int) port);
            client = new Client(ep);
            client.handshake();
        } 
        catch (Exception e) {
            log.warn("Cannot connect to serf!: {}", e);
        }
    }

    //private parseCoordinateResponse() { 
    //}

    public NetworkCoordinate getCoordinate() {
        NetworkCoordinate coordinate = new NetworkCoordinate();
        String hostName = "";
        try {
            //InetAddress IAddress = InetAddress.getLocalHost();
            //hostName = IAddress.getHostName();
            coordinate =  client.getCoordinates(nodeName).getCoordinate(); 
        }
        catch (Exception e) {
            log.warn("Cannot get coordinate for hostname {} from serf!: {}", hostName , e);
        }
        return coordinate;
    }

    public void joinNode(String server) {
        try {
            client.join(ImmutableList.<String>of(server), false);
        }
        catch (Exception e) {
            log.warn("Cannot join server  {} with Serf cluster: {}", server, e);
        }
    }

    public void joinNodes(List<String> servers) {
        try {
            client.join(servers, false);
        }
        catch (Exception e) {
            log.warn("Cannot join servers {} with Serf cluster: {}", servers, e);
        }
    } 

    public boolean checkMemberList(String nodeName) {
        try {
            List<Member> members = client.members().getMembers();
            for(Member member : members) {
                if(member.getName().equals(nodeName)) {
                    return true;
                }
            }
            return false;
        }
        catch (Exception e) {
            log.warn("Error connecting to Serf Cluster: {}", e);
            return false;
        }
    }
}
