package org.apache.pulsar.broker.loadbalance;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

import org.apache.pulsar.broker.BundleLatencyData;
import org.apache.pulsar.policies.data.loadbalancer.CetusBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.CetusNetworkCoordinateData;

/**
 * This class contains all information necessary to load balance Cetus
 * @author harshitg, tlandle
 *
 */
public class CetusLoadData {

	private ConcurrentHashMap<String, CetusBrokerData> cetusBrokerDataMap;
	
    /**
     * Initialize a CetusLoadData.
     */
    public CetusLoadData() {
        cetusBrokerDataMap = new ConcurrentHashMap<String, CetusBrokerData>(16,1);
    }
    
    public ConcurrentHashMap<String, CetusBrokerData> getCetusBrokerData() {
    	return cetusBrokerDataMap;
    }
}
