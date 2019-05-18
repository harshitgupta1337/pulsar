package no.tv2.serf.client;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.pulsar.common.policies.data.NetworkCoordinate;
import static org.junit.Assert.*;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;


/**
 *
 * @author Tyler Landle <tyler.landle@gmail.com>
 */
public class PulsarSerfTest {
    public static final Logger log = LoggerFactory.getLogger(PulsarSerfTest.class);

    public static final String SERF_RPC_IP = "0.0.0.0";
    public static final int SERF_RPC_PORT = 7373;

    private Client client;
    private SerfEndpoint endpoint;

    @Before
    public void before() throws IOException, SerfCommunicationException {
        log.info("Before");
        client = createClient(SERF_RPC_IP, SERF_RPC_PORT);
    }
    

    @After
    public void after() throws SerfCommunicationException {
        log.info("After");
        client.close();
    }
    public Client createClient(String ip, int port) throws SerfCommunicationException, IOException {
        endpoint = new SocketEndpoint(ip, port);
        Client client = new Client(endpoint);
        client.handshake();
        return client;
    }

    @Test
    public void testCoordinate() throws SerfCommunicationException,UnknownHostException {
        log.info("Test Get Coordinate");
        InetAddress IAddress = InetAddress.getLocalHost();
        String hostName = IAddress.getHostName();
        CoordinateResponse response = client.getCoordinates("n1");
        log.info("Adjustment: {} Error: {}  Height: {} CoordinateVec {}", response.getCoordinate().getAdjustment(), response.getCoordinate().getError(), response.getCoordinate().getHeight(), response.getCoordinate().getCoordinateVector());
    }
} 
