package org.apache.pulsar.broker.loadbalance;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

import org.apache.pulsar.broker.BundleLatencyData;
import org.apache.pulsar.policies.data.loadbalancer.CetusBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.CetusNetworkCoordinateData;
import org.apache.pulsar.broker.loadbalance.LoadData;

/**
 * This class contains all information necessary to load balance Cetus
 * @author harshitg, tlandle
 *
 */
public class CetusLoadData {

	private ConcurrentHashMap<String, CetusBrokerData> cetusBrokerDataMap;

    private ConcurrentHashMap<String, CetusNetworkCoordinateData> cetusBundleDataMap;

    private LoadData loadData;
	
    /**
     * Initialize a CetusLoadData.
     */
    public CetusLoadData() {
        cetusBrokerDataMap = new ConcurrentHashMap<String, CetusBrokerData>(16,1);
        cetusBundleDataMap = new ConcurrentHashMap<String, CetusNetworkCoordinateData>(16,1);
        loadData = new LoadData();
    }
    
    public ConcurrentHashMap<String, CetusBrokerData> getCetusBrokerDataMap() {
    	return cetusBrokerDataMap;
    }

     public ConcurrentHashMap<String, CetusNetworkCoordinateData> getCetusBundleDataMap() {
    	return cetusBundleDataMap;
    }

    public LoadData getLoadData() {
        return loadData;
    }

}
