package org.apache.pulsar.broker.loadbalance;

import java.util.Map;

import org.apache.pulsar.broker.BundleLatencyData;

/**
 * This class contains all information necessary to load balance Cetus
 * @author harshitg
 *
 */
public class CetusLoadData extends LoadData {

	private Map<String, BundleLatencyData> bundleLatencyData;
	
    /**
     * Initialize a CetusLoadData.
     */
    public CetusLoadData() {
        super(); 
    }
    
    public Map<String, BundleLatencyData> getBundleLatencyData() {
    	return bundleLatencyData;
    }
}
