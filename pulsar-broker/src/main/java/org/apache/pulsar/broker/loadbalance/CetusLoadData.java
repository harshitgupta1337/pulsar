package org.apache.pulsar.broker.loadbalance;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

import org.apache.pulsar.broker.BundleLatencyData;
import org.apache.pulsar.policies.data.loadbalancer.CetusLatencyMonitoringData;

import org.apache.pulsar.policies.data.loadbalancer.CetusNetworkCoordinateData;
import org.apache.pulsar.broker.loadbalance.LoadData;

/**
 * This class contains all information necessary to load balance Cetus
 * @author harshitg, tlandle
 *
 */
public class CetusLoadData {

	private ConcurrentHashMap<String, CetusLatencyMonitoringData> cetusBrokerLatencyData;

    private ConcurrentHashMap<String, CetusNetworkCoordinateData> cetusBundleDataMap;

    private LoadData loadData;
	
    /**
     * Initialize a CetusLoadData.
     */
    public CetusLoadData() {
        cetusBrokerLatencyData = new ConcurrentHashMap<String, CetusLatencyMonitoringData>(16,1);
        cetusBundleDataMap = new ConcurrentHashMap<String, CetusNetworkCoordinateData>(16,1);
        loadData = new LoadData();
    }
    
    public ConcurrentHashMap<String, CetusLatencyMonitoringData> getCetusBrokerLatencyDataMap() {
    	return cetusBrokerLatencyData;
    }

     public ConcurrentHashMap<String, CetusNetworkCoordinateData> getCetusBundleDataMap() {
    	return cetusBundleDataMap;
    }

    public LoadData getLoadData() {
        return loadData;
    }

}
