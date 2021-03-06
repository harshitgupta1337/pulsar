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
package org.apache.pulsar.broker.loadbalance.impl;

import static org.apache.pulsar.broker.admin.AdminResource.jsonMapper;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;
import static org.apache.pulsar.broker.web.PulsarWebResource.path;
import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.Gson;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.BufferedReader;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageBrokerData;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.broker.loadbalance.BundleSplitStrategy;
import org.apache.pulsar.broker.loadbalance.CetusPeriodicLoadManager;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy;
import org.apache.pulsar.broker.loadbalance.ModularLoadManager;
import org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import org.apache.pulsar.broker.loadbalance.CetusBundleUnloadingStrategy;
import org.apache.pulsar.broker.loadbalance.BrokerChange;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared.BrokerTopicLoadingPredicate;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.broker.stats.prometheus.TopicMigrationStats;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.CetusLatencyMonitoringData;
import org.apache.pulsar.policies.data.loadbalancer.CetusNetworkCoordinateData;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.apache.pulsar.zookeeper.ZooKeeperCache.Deserializer;
import org.apache.pulsar.zookeeper.ZooKeeperCacheListener;
import org.apache.pulsar.zookeeper.ZooKeeperChildrenCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.pulsar.broker.loadbalance.CetusLoadData;
import org.apache.pulsar.common.policies.data.NetworkCoordinate;
import org.apache.pulsar.common.util.CoordinateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CetusPeriodicLoadManagerImpl implements CetusPeriodicLoadManager, ZooKeeperCacheListener<CetusLatencyMonitoringData> {
    private static final Logger log = LoggerFactory.getLogger(CetusPeriodicLoadManagerImpl.class);

    private static final java.util.logging.Logger bundleStatsLog = java.util.logging.Logger.getLogger(CetusPeriodicLoadManagerImpl.class.getName());

    // Path to ZNode whose children contain BundleData jsons for each bundle (new API version of ResourceQuota).
    public static final String BUNDLE_DATA_ZPATH = "/loadbalance/bundle-data";

    public static final String CETUS_COORDINATE_DATA_ROOT = "/cetus/coordinate-data";

    // Default message rate to assume for unseen bundles.
    public static final double DEFAULT_MESSAGE_RATE = 50;

    // Default message throughput to assume for unseen bundles.
    // Note that the default message size is implicitly defined as DEFAULT_MESSAGE_THROUGHPUT / DEFAULT_MESSAGE_RATE.
    public static final double DEFAULT_MESSAGE_THROUGHPUT = 50000;

    // The number of effective samples to keep for observing long term data.
    public static final int NUM_LONG_SAMPLES = 1000;

    // The number of effective samples to keep for observing short term data.
    public static final int NUM_SHORT_SAMPLES = 10;

    // Path to ZNode whose children contain ResourceQuota jsons.
    public static final String RESOURCE_QUOTA_ZPATH = "/loadbalance/resource-quota/namespace";

    // Path to ZNode containing TimeAverageBrokerData jsons for each broker.
    public static final String TIME_AVERAGE_BROKER_ZPATH = "/loadbalance/broker-time-average";

    // ZooKeeper Cache of the currently available active brokers.
    // availableActiveBrokers.get() will return a set of the broker names without an http prefix.
    private ZooKeeperChildrenCache availableActiveBrokers;

    // Set of broker candidates to reuse so that object creation is avoided.
    private final Set<String> brokerCandidateCache;

    // ZooKeeper cache of the local broker data, stored in LoadManager.LOADBALANCE_BROKER_ROOT.
    private ZooKeeperDataCache<LocalBrokerData> brokerDataCache;

    // CETUS Zookeeper cache of the cetus broker data
    private ZooKeeperDataCache<CetusLatencyMonitoringData> cetusBrokerDataCache;

    // Broker host usage object used to calculate system resource usage.
    private BrokerHostUsage brokerHostUsage;

    // Map from brokers to namespaces to the bundle ranges in that namespace assigned to that broker.
    // Used to distribute bundles within a namespace evely across brokers.
    private final Map<String, Map<String, Set<String>>> brokerToNamespaceToBundleRange;

    // Path to the ZNode containing the LocalBrokerData json for this broker.
    private String brokerZnodePath;

    //CETUS 
    private String cetusBrokerZnodePath;

    // Strategy to use for splitting bundles.
    private BundleSplitStrategy bundleSplitStrategy;

    // Service configuration belonging to the pulsar service.
    private ServiceConfiguration conf;

    // The default bundle stats which are used to initialize historic data.
    // This data is overriden after the bundle receives its first sample.
    private final NamespaceBundleStats defaultStats;

    // Used to filter brokers from being selected for assignment.
    private final List<BrokerFilter> filterPipeline;

    // Timestamp of last invocation of updateBundleData.
    private long lastBundleDataUpdate;

    // LocalBrokerData available before most recent update.
    private LocalBrokerData lastData;

    // Pipeline used to determine what namespaces, if any, should be unloaded.
    private final List<LoadSheddingStrategy> loadSheddingPipeline;

    private final CetusBundleUnloadingStrategy bundleUnloadingStrategy;

    // Local data for the broker this is running on.
    private LocalBrokerData localData;

    // Client data for the clients connected to topics on this broker
    //private ClientData clientData;

    // CETUS Load Data for data available for each bundle
    private final CetusLoadData cetusLoadData;

    // Load data comprising data available for each broker.
    //private final LoadData cetusLoadData.getLoadData();

    // Used to determine whether a bundle is preallocated.
    private final Map<String, String> preallocatedBundleToBroker;

    public final Map<String, String> desiredBrokerForBundle;

    // Strategy used to determine where new topics should be placed.
    private ModularLoadManagerStrategy placementStrategy;

    // Policies used to determine which brokers are available for particular namespaces.
    private SimpleResourceAllocationPolicies policies;

    // Pulsar service used to initialize this.
    private PulsarService pulsar;

    // Executor service used to regularly update broker data.
    private final ScheduledExecutorService scheduler;

    // ZooKeeper belonging to the pulsar service.
    private ZooKeeper zkClient;

    // check if given broker can load persistent/non-persistent topic
    private final BrokerTopicLoadingPredicate brokerTopicLoadingPredicate;

    private Map<String, String> brokerToFailureDomainMap;

    // Cetus - Log Topic Migration Stats
    private Map<String, TopicMigrationStats> topicMigrationStats;
    private Multimap<String, Long> bundleUnloadTimes;
    private Map<String, Long> bundleUnloadStartTime;
    private ScheduledExecutorService bundleStatsPrintService;

    private static final Deserializer<LocalBrokerData> loadReportDeserializer = (key, content) -> jsonMapper()
        .readValue(content, LocalBrokerData.class);

    private static final Deserializer<CetusLatencyMonitoringData> cetusDeserializer = (key, content) -> jsonMapper()
        .readValue(content, CetusLatencyMonitoringData.class);

    private long lastLoadSheddingTime;
    private final long initTime;
    private List<String> brokersList;
    private String lastTargetBroker;

    /**
     * Initializes fields which do not depend on PulsarService. initialize(PulsarService) should subsequently be called.
     */
    public CetusPeriodicLoadManagerImpl() {
        brokerCandidateCache = new HashSet<>();
        brokerToNamespaceToBundleRange = new HashMap<>();
        defaultStats = new NamespaceBundleStats();
        filterPipeline = new ArrayList<>();
        //cetusLoadData.getLoadData() = new LoadData();
        loadSheddingPipeline = new ArrayList<>();
        loadSheddingPipeline.add(new OverloadShedder());
        bundleUnloadingStrategy = new CetusAllLoadShedder();
        preallocatedBundleToBroker = new ConcurrentHashMap<>();
        desiredBrokerForBundle = new ConcurrentHashMap<>();
        scheduler = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-modular-load-manager"));
        this.brokerToFailureDomainMap = Maps.newHashMap();
        // CETUS
        //this.brokerToProducerConsumerMap = Maps.newHashMap();
        this.cetusLoadData = new CetusLoadData();
        this.topicMigrationStats = new HashMap<>();
        this.bundleUnloadTimes = ArrayListMultimap.create();
        this.bundleUnloadStartTime = new HashMap<>();
        this.bundleStatsPrintService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("broker-print"));
        this.lastLoadSheddingTime = 0;
        this.initTime = System.currentTimeMillis();
        this.brokersList = null;
        this.lastTargetBroker = null;
        
        this.brokerTopicLoadingPredicate = new BrokerTopicLoadingPredicate() {
            @Override
                public boolean isEnablePersistentTopics(String brokerUrl) {
                    final BrokerData brokerData = cetusLoadData.getLoadData().getBrokerData().get(brokerUrl.replace("http://", ""));
                    return brokerData != null && brokerData.getLocalData() != null
                        && brokerData.getLocalData().isPersistentTopicsEnabled();
                }

            @Override
                public boolean isEnableNonPersistentTopics(String brokerUrl) {
                    final BrokerData brokerData = cetusLoadData.getLoadData().getBrokerData().get(brokerUrl.replace("http://", ""));
                    return brokerData != null && brokerData.getLocalData() != null
                        && brokerData.getLocalData().isNonPersistentTopicsEnabled();
                }
        };
    }

    /**
     * Initialize this load manager using the given PulsarService. Should be called only once, after invoking the
     * default constructor.
     *
     * @param pulsar
     *            The service to initialize with.
     */
    public void initialize(final PulsarService pulsar) {
        this.pulsar = pulsar;
        availableActiveBrokers = new ZooKeeperChildrenCache(pulsar.getLocalZkCache(),
                LoadManager.LOADBALANCE_BROKERS_ROOT);
        //scheduler.scheduleAtFixedRate(safeRun(() -> writeBrokerDataOnZooKeeper()), 0, 1000, TimeUnit.MILLISECONDS);
        //scheduler.scheduleAtFixedRate(safeRun(() -> writeBundleDataOnZooKeeper()), 0, 1000, TimeUnit.MILLISECONDS);
        availableActiveBrokers.registerListener(new ZooKeeperCacheListener<Set<String>>() {
                @Override
                public void onUpdate(String path, Set<String> data, Stat stat) {
                if (log.isDebugEnabled()) {
                log.debug("Update Received for path {}", path);
                }
                reapDeadBrokerPreallocations(data);
                scheduler.submit(CetusPeriodicLoadManagerImpl.this::updateAll);
                }
                });

        brokerDataCache = new ZooKeeperDataCache<LocalBrokerData>(pulsar.getLocalZkCache()) {
            @Override
                public LocalBrokerData deserialize(String key, byte[] content) throws Exception {
                    return ObjectMapperFactory.getThreadLocal().readValue(content, LocalBrokerData.class);
                }
        };

        conf = pulsar.getConfiguration();
        //brokerDataCache.registerListener(this);
       
        cetusBrokerDataCache = new ZooKeeperDataCache<CetusLatencyMonitoringData>(pulsar.getLocalZkCache()) {
            @Override
                public CetusLatencyMonitoringData deserialize(String key, byte[] content) throws Exception {
                    return ObjectMapperFactory.getThreadLocal().readValue(content, CetusLatencyMonitoringData.class);
                }
        };
        cetusBrokerDataCache.registerListener(this);

        if (SystemUtils.IS_OS_LINUX) {
            brokerHostUsage = new LinuxBrokerHostUsageImpl(pulsar);
        } else {
            brokerHostUsage = new GenericBrokerHostUsageImpl(pulsar);
        }

        bundleSplitStrategy = new BundleSplitterTask(pulsar);

        // Initialize the default stats to assume for unseen bundles (hard-coded for now).
        defaultStats.msgThroughputIn = DEFAULT_MESSAGE_THROUGHPUT;
        defaultStats.msgThroughputOut = DEFAULT_MESSAGE_THROUGHPUT;
        defaultStats.msgRateIn = DEFAULT_MESSAGE_RATE;
        defaultStats.msgRateOut = DEFAULT_MESSAGE_RATE;

        lastData = new LocalBrokerData(pulsar.getWebServiceAddress(), pulsar.getWebServiceAddressTls(),
                pulsar.getBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls());
        localData = new LocalBrokerData(pulsar.getWebServiceAddress(), pulsar.getWebServiceAddressTls(),
                pulsar.getBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls());
        localData.setBrokerVersionString(pulsar.getBrokerVersion());
        // configure broker-topic mode
        lastData.setPersistentTopicsEnabled(pulsar.getConfiguration().isEnablePersistentTopics());
        lastData.setNonPersistentTopicsEnabled(pulsar.getConfiguration().isEnableNonPersistentTopics());
        localData.setPersistentTopicsEnabled(pulsar.getConfiguration().isEnablePersistentTopics());
        localData.setNonPersistentTopicsEnabled(pulsar.getConfiguration().isEnableNonPersistentTopics());


        placementStrategy = ModularLoadManagerStrategy.create(conf);
        policies = new SimpleResourceAllocationPolicies(pulsar);
        zkClient = pulsar.getZkClient();
        filterPipeline.add(new BrokerVersionFilter());

        refreshBrokerToFailureDomainMap();
        // register listeners for domain changes
        pulsar.getConfigurationCache().failureDomainListCache()
            .registerListener((path, data, stat) -> scheduler.execute(() -> refreshBrokerToFailureDomainMap()));
        pulsar.getConfigurationCache().failureDomainCache()
            .registerListener((path, data, stat) -> scheduler.execute(() -> refreshBrokerToFailureDomainMap()));
        startBundleStatsPrint();
    }

    /**
     * Initialize this load manager.
     *
     * @param pulsar
     *            Client to construct this manager from.
     */
    public CetusPeriodicLoadManagerImpl(final PulsarService pulsar) {
        this();
        initialize(pulsar);
    }

    void startBundleStatsPrint() {
        log.info("Starting bundle stats service");
        try {
            FileHandler fh = new FileHandler("/home/tyler/pulsar/bundlestatslog.log");  
            bundleStatsLog.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();  
            fh.setFormatter(formatter);  
            bundleStatsPrintService.scheduleAtFixedRate(safeRun(() -> printBundleStats()), 100, 1000,TimeUnit.MILLISECONDS);
        }
        catch (Exception e) {
            log.warn("Cannot setup bundle stats log");
        }
    }

    void printBundleStats() {
        //if (pulsar.getLeaderElectionService().isLeader()) {
        try {
            FileWriter unloadTimesFile = new FileWriter("/home/cetususer/pulsar/bundle_unload_times.json");
            FileWriter unloadTopicTimesAvgFile = new FileWriter("/home/cetususer/pulsar/bundle_unload_avg.json");
            FileWriter unloadTotalAvgFile = new FileWriter("/home/tyler/cetususer/bundle_unload_total_avg.json");
            //bundleStatsLog.info("Bundle Stats: " +bundleUnloadTimes.toString());
            Gson gson = new Gson();
            String json = gson.toJson(bundleUnloadTimes.asMap());
            log.info("Bundle Json: {}", json);
            unloadTimesFile.write(json);
            Map<String, Double> averages = new HashMap<>();
            List<Double> averagesList = new ArrayList<>();
            for(String key : bundleUnloadTimes.keySet()) {
                Collection<Long> values = bundleUnloadTimes.get(key);
                Double average = values.stream().mapToLong(val -> val).average().orElse(0.0);
                averages.put(key, average);
                averagesList.add(average);
            }
            //bundleStatsLog.info("Bundle Stats Avg: " +averages);
            json = gson.toJson(averages);
            unloadTopicTimesAvgFile.write(json);
            Double totalAverage = averagesList.stream().mapToDouble(val -> val).average().orElse(0.0);
            json = gson.toJson(totalAverage);
            unloadTotalAvgFile.write(json);
            unloadTimesFile.flush();
            unloadTopicTimesAvgFile.flush();
            unloadTotalAvgFile.flush();
            bundleStatsLog.info("Bundle Stats Total Avg: " +totalAverage);


        }
        catch (Exception e) {
        }



        //}
    } 

    // Attempt to create a ZooKeeper path if it does not exist.
    private static void createZPathIfNotExists(final ZooKeeper zkClient, final String path) throws Exception {
        if (zkClient.exists(path, false) == null) {
            try {
                ZkUtils.createFullPathOptimistic(zkClient, path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // Ignore if already exists.
            }
        }
    }

    // For each broker that we have a recent load report, see if they are still alive
    private void reapDeadBrokerPreallocations(Set<String> aliveBrokers) {
        for ( String broker : cetusLoadData.getLoadData().getBrokerData().keySet() ) {
            if ( !aliveBrokers.contains(broker)) {
                if ( log.isDebugEnabled() ) {
                    log.debug("Broker {} appears to have stopped; now reclaiming any preallocations", broker);
                }
                final Iterator<Map.Entry<String, String>> iterator = preallocatedBundleToBroker.entrySet().iterator();
                while ( iterator.hasNext() ) {
                    Map.Entry<String, String> entry = iterator.next();
                    final String preallocatedBundle = entry.getKey();
                    final String preallocatedBroker = entry.getValue();
                    if ( broker.equals(preallocatedBroker) ) {
                        if ( log.isDebugEnabled() ) {
                            log.debug("Removing old preallocation on dead broker {} for bundle {}",
                                    preallocatedBroker, preallocatedBundle);
                        }
                        iterator.remove();
                    }
                }
            }
        }
    }

    @Override
        public Set<String> getAvailableBrokers() {
            try {
                return availableActiveBrokers.get();
            } catch (Exception e) {
                log.warn("Error when trying to get active brokers", e);
                return cetusLoadData.getLoadData().getBrokerData().keySet();
            }
        }

    // Attempt to local the data for the given bundle in ZooKeeper.
    // If it cannot be found, return the default bundle data.
    private BundleData getBundleDataOrDefault(final String bundle) {
        BundleData bundleData = null;
        try {
            final String bundleZPath = getBundleDataZooKeeperPath(bundle);
            final String quotaZPath = String.format("%s/%s", RESOURCE_QUOTA_ZPATH, bundle);
            if (zkClient.exists(bundleZPath, null) != null) {
                bundleData = readJson(zkClient.getData(bundleZPath, null, null), BundleData.class);
            } else if (zkClient.exists(quotaZPath, null) != null) {
                final ResourceQuota quota = readJson(zkClient.getData(quotaZPath, null, null), ResourceQuota.class);
                bundleData = new BundleData(NUM_SHORT_SAMPLES, NUM_LONG_SAMPLES);
                // Initialize from existing resource quotas if new API ZNodes do not exist.
                final TimeAverageMessageData shortTermData = bundleData.getShortTermData();
                final TimeAverageMessageData longTermData = bundleData.getLongTermData();

                shortTermData.setMsgRateIn(quota.getMsgRateIn());
                shortTermData.setMsgRateOut(quota.getMsgRateOut());
                shortTermData.setMsgThroughputIn(quota.getBandwidthIn());
                shortTermData.setMsgThroughputOut(quota.getBandwidthOut());

                longTermData.setMsgRateIn(quota.getMsgRateIn());
                longTermData.setMsgRateOut(quota.getMsgRateOut());
                longTermData.setMsgThroughputIn(quota.getBandwidthIn());
                longTermData.setMsgThroughputOut(quota.getBandwidthOut());

                // Assume ample history.
                shortTermData.setNumSamples(NUM_SHORT_SAMPLES);
                longTermData.setNumSamples(NUM_LONG_SAMPLES);
            }
        } catch (Exception e) {
            log.warn("Error when trying to find bundle {} on zookeeper: {}", bundle, e);
        }
        if (bundleData == null) {
            bundleData = new BundleData(NUM_SHORT_SAMPLES, NUM_LONG_SAMPLES, defaultStats);
        }
        return bundleData;
    }

    // Get the ZooKeeper path for the given bundle full name.
    private static String getBundleDataZooKeeperPath(final String bundle) {
        return BUNDLE_DATA_ZPATH + "/" + bundle;
    }

    // Use the Pulsar client to acquire the namespace bundle stats.
    private Map<String, NamespaceBundleStats> getBundleStats() {
        return pulsar.getBrokerService().getBundleStats();
    }

    // Use the thread local ObjectMapperFactory to read the given json data into an instance of the given class.
    private static <T> T readJson(final byte[] data, final Class<T> clazz) throws IOException {
        return ObjectMapperFactory.getThreadLocal().readValue(data, clazz);
    }

    private double percentChange(final double oldValue, final double newValue) {
        if (oldValue == 0) {
            if (newValue == 0) {
                // Avoid NaN
                return 0;
            }
            return Double.POSITIVE_INFINITY;
        }
        return 100 * Math.abs((oldValue - newValue) / oldValue);
    }

    // Determine if the broker data requires an update by delegating to the update condition.
    private boolean needBrokerDataUpdate() {
        final long updateMaxIntervalMillis = TimeUnit.MINUTES
            .toMillis(conf.getLoadBalancerReportUpdateMaxIntervalMinutes());
        long timeSinceLastReportWrittenToZooKeeper = System.currentTimeMillis() - localData.getLastUpdate();
        if (timeSinceLastReportWrittenToZooKeeper > updateMaxIntervalMillis) {
            log.info("Writing local data to ZooKeeper because time since last update exceeded threshold of {} minutes",
                    conf.getLoadBalancerReportUpdateMaxIntervalMinutes());
            // Always update after surpassing the maximum interval.
            return true;
        }
        final double maxChange = Math
            .max(100.0 * (Math.abs(lastData.getMaxResourceUsage() - localData.getMaxResourceUsage())),
                    Math.max(percentChange(lastData.getMsgRateIn() + lastData.getMsgRateOut(),
                            localData.getMsgRateIn() + localData.getMsgRateOut()),
                        Math.max(
                            percentChange(lastData.getMsgThroughputIn() + lastData.getMsgThroughputOut(),
                                localData.getMsgThroughputIn() + localData.getMsgThroughputOut()),
                            percentChange(lastData.getNumBundles(), localData.getNumBundles()))));
        if (maxChange > conf.getLoadBalancerReportUpdateThresholdPercentage()) {
            log.info("Writing local data to ZooKeeper because maximum change {}% exceeded threshold {}%; " +
                    "time since last report written is {} seconds", maxChange,
                    conf.getLoadBalancerReportUpdateThresholdPercentage(), timeSinceLastReportWrittenToZooKeeper/1000.0);
            return true;
        }
        return false;
    }

    // Update both the broker data and the bundle data.
    public void updateAll() {
        if (log.isDebugEnabled()) {
            log.debug("Updating broker and bundle data for loadreport");
        }
        updateAllBrokerData();
        updateBundleData();
        updateLatencyData();

        // broker has latest load-report: check if any bundle requires split
        //checkNamespaceBundleSplit();
    }

    private void updateLatencyData() {
        final Set<String> activeBrokers = getAvailableBrokers();
        for (String broker : activeBrokers) {
            try {
                String key = String.format("%s/%s", CETUS_COORDINATE_DATA_ROOT, broker);
                final CetusLatencyMonitoringData cetusLocalData = cetusBrokerDataCache.get(key)
                    .orElseThrow(KeeperException.NoNodeException::new);

                if(cetusLoadData.getCetusBrokerLatencyDataMap().containsKey(broker)) {
                    cetusLoadData.getCetusBrokerLatencyDataMap().put(broker, cetusLocalData);
                }
                else {
                    String[] brokerIp = broker.split(":");
                    String serfString = String.format("%s:%s", brokerIp[0], pulsar.getSerfBindPort());
                    log.info("Joining Serf Node: {}", serfString);
                    pulsar.getSerfClient().joinNode(brokerIp[0]);
                    cetusLoadData.getCetusBrokerLatencyDataMap().put(broker, new CetusLatencyMonitoringData(cetusLocalData)); 
                }
                
                // Clearing the cetusLoadData bundle data map
                // So that it is overwritten by recvd data
                // TODO cetusLoadData.getCetusBundleDataMap().clear();

                for(Map.Entry<String, CetusNetworkCoordinateData> entry : cetusLocalData.getBundleNetworkCoordinates().entrySet()) {
                    if(cetusLoadData.getCetusBundleDataMap().containsKey(entry.getKey())) {
                        cetusLoadData.getCetusBundleDataMap().put(entry.getKey(), entry.getValue());
                    }
                    else {
                        cetusLoadData.getCetusBundleDataMap().put(entry.getKey(), new CetusNetworkCoordinateData());
                    }
                }
            }
            catch (NoNodeException ne){
                log.debug("Couldn't get broker data, removing from map: {}", ne);
                cetusLoadData.getCetusBrokerLatencyDataMap().remove(broker);
                log.warn("[{}] broker load-report znode not present", broker, ne);
            } 
            catch (Exception e) {
                log.warn("Error reading broker data from cache for broker - [{}], [{}]", broker, e.getMessage());
            }

        }
        // Remove obsolete brokers.
        for (final String broker : cetusLoadData.getCetusBrokerLatencyDataMap().keySet()) {
            if (!activeBrokers.contains(broker)) {
                cetusLoadData.getCetusBrokerLatencyDataMap().remove(broker);
            }
        }
    }

    // As the leader broker, update the broker data map in cetusLoadData.getLoadData() by querying ZooKeeper for the broker data put there
    // by each broker via updateLocalBrokerData.
    private void updateAllBrokerData() {
        final Set<String> activeBrokers = getAvailableBrokers();
        final Map<String, BrokerData> brokerDataMap = cetusLoadData.getLoadData().getBrokerData();
        for (String broker : activeBrokers) {
            try {
                String key = String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, broker);
                final LocalBrokerData localData = brokerDataCache.get(key)
                    .orElseThrow(KeeperException.NoNodeException::new);

                if (brokerDataMap.containsKey(broker)) {
                    // Replace previous local broker data.
                    brokerDataMap.get(broker).setLocalData(localData);
                    log.info("Updating message rates for broker {}. In = {},{} ; Out = {},{}", broker, localData.getMsgThroughputIn(), localData.getMsgRateIn(), localData.getMsgThroughputOut(), localData.getMsgRateOut());
                } else {
                    // Initialize BrokerData object for previously unseen
                    // brokers.
                    brokerDataMap.put(broker, new BrokerData(localData));
                }
            } catch (NoNodeException ne) {
                // it only happens if we update-brokerData before availableBrokerCache refreshed with latest data and
                // broker's delete-znode watch-event hasn't updated availableBrokerCache
                brokerDataMap.remove(broker);
                log.warn("[{}] broker load-report znode not present", broker, ne);
            } catch (Exception e) {
                log.warn("Error reading broker data from cache for broker - [{}], [{}]", broker, e.getMessage());
            }
        }
        // Remove obsolete brokers.
        for (final String broker : brokerDataMap.keySet()) {
            if (!activeBrokers.contains(broker)) {
                brokerDataMap.remove(broker);
            }
        }
    }

    // As the leader broker, use the local broker data saved on ZooKeeper to update the bundle stats so that better load
    // management decisions may be made.
    private void updateBundleData() {
        final Map<String, BundleData> bundleData = cetusLoadData.getLoadData().getBundleData();
        // Iterate over the broker data.
        for (Map.Entry<String, BrokerData> brokerEntry : cetusLoadData.getLoadData().getBrokerData().entrySet()) {
            final String broker = brokerEntry.getKey();
            final BrokerData brokerData = brokerEntry.getValue();
            final Map<String, NamespaceBundleStats> statsMap = brokerData.getLocalData().getLastStats();

            // Iterate over the last bundle stats available to the current
            // broker to update the bundle data.
            for (Map.Entry<String, NamespaceBundleStats> entry : statsMap.entrySet()) {
                final String bundle = entry.getKey();
                final NamespaceBundleStats stats = entry.getValue();
                if (bundleData.containsKey(bundle)) {
                    // If we recognize the bundle, add these stats as a new
                    // sample.
                    bundleData.get(bundle).update(stats);
                } else {
                    // Otherwise, attempt to find the bundle data on ZooKeeper.
                    // If it cannot be found, use the latest stats as the first
                    // sample.
                    BundleData currentBundleData = getBundleDataOrDefault(bundle);
                    currentBundleData.update(stats);
                    bundleData.put(bundle, currentBundleData);
                }
            }

            // Remove all loaded bundles from the preallocated maps.
            final Map<String, BundleData> preallocatedBundleData = brokerData.getPreallocatedBundleData();
            synchronized (preallocatedBundleData) {
                for (String preallocatedBundleName : brokerData.getPreallocatedBundleData().keySet()) {
                    if (brokerData.getLocalData().getBundles().contains(preallocatedBundleName)) {
                        final Iterator<Map.Entry<String, BundleData>> preallocatedIterator = preallocatedBundleData.entrySet()
                            .iterator();
                        while (preallocatedIterator.hasNext()) {
                            final String bundle = preallocatedIterator.next().getKey();

                            if (bundleData.containsKey(bundle)) {
                                preallocatedIterator.remove();
                                preallocatedBundleToBroker.remove(bundle);
                            }
                        }
                    }

                    // This is needed too in case a broker which was assigned a bundle dies and comes back up.
                    if ( preallocatedBundleToBroker.containsKey(preallocatedBundleName) ) {
                        preallocatedBundleToBroker.remove(preallocatedBundleName);
                    }
                }
            }

            // Using the newest data, update the aggregated time-average data for the current broker.
            brokerData.getTimeAverageData().reset(statsMap.keySet(), bundleData, defaultStats);
            final Map<String, Set<String>> namespaceToBundleRange = brokerToNamespaceToBundleRange
                .computeIfAbsent(broker, k -> new HashMap<>());
            synchronized (namespaceToBundleRange) {
                namespaceToBundleRange.clear();
                LoadManagerShared.fillNamespaceToBundlesMap(statsMap.keySet(), namespaceToBundleRange);
                LoadManagerShared.fillNamespaceToBundlesMap(preallocatedBundleData.keySet(), namespaceToBundleRange);
            }
        }
    }

    /**
     * As any broker, disable the broker this manager is running on.
     *
     * @throws PulsarServerException
     *             If ZooKeeper failed to disable the broker.
     */
    @Override
        public void disableBroker() throws PulsarServerException {
            if (StringUtils.isNotEmpty(brokerZnodePath)) {
                try {
                    pulsar.getZkClient().delete(brokerZnodePath, -1);
                } catch (Exception e) {
                    throw new PulsarServerException(e);
                }
            }
        }

        public String readNextTargetBroker() {
            String broker = null;
            try {
                FileReader r = new FileReader(CetusPeriodicLoadManager.TARGET_BROKER);
                BufferedReader reader = new BufferedReader(r);
                broker = reader.readLine();
                broker = broker.trim();
                r.close();
            } catch (IOException e) {
                return null;
            }
            if (broker.equals("")) return null;
            return broker;
        }

    /**
     * As the leader broker, select bundles for the namespace service to unload so that they may be reassigned to new
     * brokers.
     */

    @Override
        public synchronized void doLoadShedding() {
            if (getAvailableBrokers().size() <= 1) {
                log.info("Only 1 broker available: no load shedding will be performed");
                return;
            }

            long currTime = System.currentTimeMillis();
            /** 
             * If this is first call to this function, 
             */
            if (currTime - initTime <= CetusPeriodicLoadManager.WARMUP_SECS*1000) {
                log.debug("Warmup not done. Only {} seconds have passed since init.", (currTime - initTime)/1000);
                return;
            }

            
            if (this.brokersList == null) {
                // Broker set hasn't been populated yet. 
                // TODO IMPORTANT : This also means that once this list is populated, it cannot be changed
                // We need to populate a sorted list of brokers that does not change
                this.brokersList = new ArrayList<String>();
                String httpPrefix = "http://";
                String selfUrl = pulsar.getWebServiceAddress().substring(httpPrefix.length());
                log.info("PERIODIC_LB SELFURL : {}", selfUrl);
                log.info("PERIODIC_LB BrokerDataMap size : {}", cetusLoadData.getCetusBrokerLatencyDataMap().size());
                for(Map.Entry<String, CetusLatencyMonitoringData> brokerEntry : cetusLoadData.getCetusBrokerLatencyDataMap().entrySet()) {
                    // Also exclude this broker (load manager)
                    log.info("PERIODIC_LB SELFURL : {} BROKERENTRY : {}", selfUrl, brokerEntry.getKey());
                    if (selfUrl.equals(brokerEntry.getKey()))
                        continue;
                    this.brokersList.add(brokerEntry.getKey());
                }
                // Now sort the list to create a deterministic order
                Collections.sort(this.brokersList);
                  
                // Write the broker list to /tmp/brokerlist.txt
                try {
                    FileWriter w = new FileWriter(CetusPeriodicLoadManager.BROKER_LIST_LOC); 
                    for (String brokerUrl : this.brokersList) {
                        w.write(brokerUrl+"\n");
                    }
                    w.flush();
                    w.close();
                } catch (IOException e) {
                    System.err.println("Error writing brokers list");
                    e.printStackTrace();
                }
                  
            } else {
                String nextTargetBroker = readNextTargetBroker();
                log.info("PERIODIC_LB ; nextTargetBroker = {}", nextTargetBroker);
                if (nextTargetBroker == null) return;
                if (this.lastTargetBroker != null && this.lastTargetBroker.equals(nextTargetBroker)) return;

                Multimap<String, BrokerChange> bundlesToUnload;
                bundlesToUnload = bundleUnloadingStrategy.findBundlesForUnloading(cetusLoadData, conf, pulsar.getWebServiceAddress(), conf.getCetusBrokerSelectionStrategy());
                log.info("Bundles to Unload: {}", bundlesToUnload.asMap());

                String nextBroker = nextTargetBroker;
                bundlesToUnload.asMap().forEach((currBroker, brokerChanges) -> {
                        brokerChanges.forEach(brokerChange -> {
                            String bundle = brokerChange.bundle;
                            final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
                            final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
                            if (!shouldAntiAffinityNamespaceUnload(namespaceName, bundleRange, currBroker)) {
                            return;
                            }
                            // TODO Do we still need the recently unloaded condition ?
                            if(!nextBroker.equals(currBroker))
                            {
                                long startTime = System.nanoTime();
                                bundleUnloadStartTime.put(bundle, startTime);

                                log.info("Initiating migration of Bundle {} Broker {} ---> Broker {} at ts = {}", bundle, currBroker, nextBroker, currTime);
                                this.desiredBrokerForBundle.put(bundle, nextBroker);
                                pulsar.getUnloadExecutor().execute(() -> performUnloading(currBroker, bundle, bundleRange, namespaceName, nextBroker));
                            }
                            });
                        });
                this.lastTargetBroker = nextTargetBroker;
                this.lastLoadSheddingTime = System.currentTimeMillis();
            }
        }

    private void performUnloading(String broker, String bundle, String bundleRange, String namespaceName, String nextBroker) {
        log.info("[Cetus Bundle Unload Strategy] Unloading bundle: {} from broker {} to broker {} at ts = {}", bundle, broker, nextBroker, System.currentTimeMillis());
        String[] brokerSplit = nextBroker.split(":");
        try {
            pulsar.getAdminClient().namespaces().unloadNamespaceBundle(namespaceName, bundleRange, brokerSplit[0]);
        } catch (PulsarServerException | PulsarAdminException e) {
            log.warn("Error when trying to perform load shedding on {} for broker {}", bundle, broker, e);
        }
    }

    public boolean shouldAntiAffinityNamespaceUnload(String namespace, String bundle, String currentBroker) {
        try {
            Optional<Policies> nsPolicies = pulsar.getConfigurationCache().policiesCache()
                .get(path(POLICIES, namespace));
            if (!nsPolicies.isPresent() || StringUtils.isBlank(nsPolicies.get().antiAffinityGroup)) {
                return true;
            }

            synchronized (brokerCandidateCache) {
                brokerCandidateCache.clear();
                ServiceUnitId serviceUnit = pulsar.getNamespaceService().getNamespaceBundleFactory()
                    .getBundle(namespace, bundle);
                LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache,
                        getAvailableBrokers(), brokerTopicLoadingPredicate);
                return LoadManagerShared.shouldAntiAffinityNamespaceUnload(namespace, bundle, currentBroker, pulsar,
                        brokerToNamespaceToBundleRange, brokerCandidateCache);
            }

        } catch (Exception e) {
            log.warn("Failed to check anti-affinity namespace ownership for {}/{}/{}, {}", namespace, bundle,
                    currentBroker, e.getMessage());

        }
        return true;
    }

    /**
     * As the leader broker, attempt to automatically detect and split hot namespace bundles.
     */
    @Override
        public void checkNamespaceBundleSplit() {

            if (!conf.isLoadBalancerAutoBundleSplitEnabled() || pulsar.getLeaderElectionService() == null
                    || !pulsar.getLeaderElectionService().isLeader()) {
                return;
            }
            final boolean unloadSplitBundles = pulsar.getConfiguration().isLoadBalancerAutoUnloadSplitBundlesEnabled();
            synchronized (bundleSplitStrategy) {
                final Set<String> bundlesToBeSplit = bundleSplitStrategy.findBundlesToSplit(cetusLoadData.getLoadData(), pulsar);
                NamespaceBundleFactory namespaceBundleFactory = pulsar.getNamespaceService().getNamespaceBundleFactory();
                for (String bundleName : bundlesToBeSplit) {
                    try {
                        final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundleName);
                        final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundleName);
                        if (!namespaceBundleFactory
                                .canSplitBundle(namespaceBundleFactory.getBundle(namespaceName, bundleRange))) {
                            continue;
                        }
                        log.info("Load-manager splitting bundle {} and unloading {}", bundleName, unloadSplitBundles);
                        pulsar.getAdminClient().namespaces().splitNamespaceBundle(namespaceName, bundleRange,
                                unloadSplitBundles);
                        // Make sure the same bundle is not selected again.
                        cetusLoadData.getLoadData().getBundleData().remove(bundleName);
                        localData.getLastStats().remove(bundleName);
                        pulsar.getCetusBrokerData().getBundleNetworkCoordinates().remove(bundleName);
                        // Clear namespace bundle-cache
                        this.pulsar.getNamespaceService().getNamespaceBundleFactory()
                            .invalidateBundleCache(NamespaceName.get(namespaceName));
                        deleteBundleDataFromZookeeper(bundleName);
                        log.info("Successfully split namespace bundle {}", bundleName);
                    } catch (Exception e) {
                        log.error("Failed to split namespace bundle {}", bundleName, e);
                    }
                }
            }

        }

    /**
     * When the broker data ZooKeeper nodes are updated, update the broker data map.
     */
    @Override
        public void onUpdate(final String path, final CetusLatencyMonitoringData data, final Stat stat) {
            if (!pulsar.getLeaderElectionService().isLeader()) {
                return;
            }

            scheduler.submit(this::updateAll);
        }

// CETUS broker assignment selection - narrow down to specific singlular broker chosen
// by our algorithm.
public Optional<String> selectBrokerForAssignment(final ServiceUnitId serviceUnit) {
    log.info("Selecting broker for assignment");
    synchronized(brokerCandidateCache) {
        final String bundle = serviceUnit.toString();
        if(preallocatedBundleToBroker.containsKey(bundle)) {
            return Optional.of(preallocatedBundleToBroker.get(bundle));
        }
        //Optional<String> broker = Optional.of("1");
        final BundleData data = cetusLoadData.getLoadData().getBundleData().computeIfAbsent(bundle,
                key -> getBundleDataOrDefault(bundle));
        brokerCandidateCache.clear();

        getBrokersMeetLatency(bundle, CetusPeriodicLoadManager.CETUS_LATENCY_BOUND_MS); //ms

        //LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache, getAvailableBrokers(),
        //brokerTopicLoadingPredicate);

        /* Removing extra checks for now
        // filter brokers which owns topic higher than threshold
        LoadManagerShared.filterBrokersWithLargeTopicCount(brokerCandidateCache, cetusLoadData.getLoadData(),
                conf.getLoadBalancerBrokerMaxTopics());

        // distribute namespaces to domain and brokers according to anti-affinity-group
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar, serviceUnit.toString(), brokerCandidateCache,
                brokerToNamespaceToBundleRange, brokerToFailureDomainMap);
        // distribute bundles evenly to candidate-brokers

        LoadManagerShared.removeMostServicingBrokersForNamespace(serviceUnit.toString(), brokerCandidateCache,
                brokerToNamespaceToBundleRange);
        */
        

        /* NOT SURE WHY THIS PIECE OF CODE WAS PRESENT
        for(Map.Entry<String, CetusLatencyMonitoringData> brokerEntry : cetusLoadData.getCetusBrokerLatencyDataMap().entrySet()) {
            if (brokerEntry.getKey().contains("d0"))
                brokerCandidateCache.add(brokerEntry.getKey()); 
        }*/

        log.info("{} brokers being considered for assignment of {}", brokerCandidateCache.size(), bundle);

        // Use the filter pipeline to finalize broker candidates.
        try {
            for (BrokerFilter filter : filterPipeline) {
                filter.filter(brokerCandidateCache, data, cetusLoadData.getLoadData(), conf);
            }
        } catch ( BrokerFilterException x ) {
            // restore the list of brokers to the full set
            LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache, getAvailableBrokers(),
                    brokerTopicLoadingPredicate);
        }

        if ( brokerCandidateCache.isEmpty() ) {
            // restore the list of brokers to the full set
            LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache, getAvailableBrokers(),
                    brokerTopicLoadingPredicate);
        }

        // Choose a broker among the potentially smaller filtered list, when possible
        Optional<String> broker = placementStrategy.selectBroker(brokerCandidateCache, data, cetusLoadData.getLoadData(), conf);
        if (log.isDebugEnabled()) {
            log.debug("Selected broker {} from candidate brokers {}", broker, brokerCandidateCache);
        }

        if (!broker.isPresent()) {
            // No brokers available
            return broker;
        }

        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        final double maxUsage = cetusLoadData.getLoadData().getBrokerData().get(broker.get()).getLocalData().getMaxResourceUsage();
        if (maxUsage > overloadThreshold) {
            // All brokers that were in the filtered list were overloaded, so check if there is a better broker
            LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache, getAvailableBrokers(),
                    brokerTopicLoadingPredicate);
            broker = placementStrategy.selectBroker(brokerCandidateCache, data, cetusLoadData.getLoadData(), conf);
        }



        cetusLoadData.getLoadData().getBrokerData().get(broker.get()).getPreallocatedBundleData().put(bundle, data);
        preallocatedBundleToBroker.put(bundle, broker.get());

        final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
        final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
        brokerToNamespaceToBundleRange.get(broker.get()).computeIfAbsent(namespaceName, k -> new HashSet<>())
            .add(bundleRange);
        log.info("Broker selected: {}", broker);
        long endTime = System.nanoTime();
        if(bundleUnloadStartTime.containsKey(bundle)) {
            long startTime = bundleUnloadStartTime.get(bundle);
            bundleUnloadTimes.put(bundle, (endTime - startTime));
        }
        return broker;

    } 
}

private void getBrokersMeetLatency(final String bundle, int latencyReqMs) {
    Optional<String> minDistanceBroker = Optional.of("");
    String desiredBroker = this.desiredBrokerForBundle.get(bundle);
    if (desiredBroker != null) {
        brokerCandidateCache.add(desiredBroker); 
        return;
    }

    log.info("Getting broker with least latency. Brokers to select from: {}", cetusLoadData.getCetusBrokerLatencyDataMap().size());
    for(Map.Entry<String, CetusLatencyMonitoringData> brokerEntry : cetusLoadData.getCetusBrokerLatencyDataMap().entrySet()) {
        
        if(cetusLoadData.getCetusBundleDataMap().containsKey(bundle)) {
            log.info("Attempting to find closer broker: {} Distance: {}", brokerEntry.getKey(), CoordinateUtil.calculateDistance(cetusLoadData.getCetusBundleDataMap().get(bundle).getProducerConsumerAvgCoordinate(), brokerEntry.getValue().getBrokerNwCoordinate()));

            if((CoordinateUtil.calculateDistance(cetusLoadData.getCetusBundleDataMap().get(bundle).getProducerConsumerAvgCoordinate(), brokerEntry.getValue().getBrokerNwCoordinate()) * 1000) < latencyReqMs) { // 1000 is seconds to millisecods
                log.info("Bundle broker found: {}  Distance : {}", brokerEntry.getKey(), CoordinateUtil.calculateDistance(cetusLoadData.getCetusBundleDataMap().get(bundle).getProducerConsumerAvgCoordinate(), brokerEntry.getValue().getBrokerNwCoordinate()));
                brokerCandidateCache.add(brokerEntry.getKey()); 
            }
        }
        else {
            log.info("Choosing first broker available. Bundle Map Size: {}, bundle: {} brokerZnode: {} ", cetusLoadData.getCetusBundleDataMap().size(), bundle, cetusBrokerZnodePath);

            cetusLoadData.getCetusBundleDataMap().put(bundle, new CetusNetworkCoordinateData());
            if(cetusLoadData.getCetusBundleDataMap().containsKey(bundle)) {
                log.info("Attempting to find closer broker: {} Distance: {}", brokerEntry.getKey(), CoordinateUtil.calculateDistance(cetusLoadData.getCetusBundleDataMap().get(bundle).getProducerConsumerAvgCoordinate(), brokerEntry.getValue().getBrokerNwCoordinate()));

                if(CoordinateUtil.calculateDistance(cetusLoadData.getCetusBundleDataMap().get(bundle).getProducerConsumerAvgCoordinate(), brokerEntry.getValue().getBrokerNwCoordinate()) * 1000 < latencyReqMs) {
                    //log.info("Bundle broker found: {}  Distance : {}", brokerEntry.getKey(), CoordinateUtil.calculateDistance(cetusLoadData.getCetusBundleDataMap().get(bundle).getProducerConsumerAvgCoordinate(), brokerEntry.getValue().getBrokerNwCoordinate()));
                    brokerCandidateCache.add(brokerEntry.getKey()); 
                }
            }
        }

    }
}

/**
 * As any broker, start the load manager.
 *
 * @throws PulsarServerException
 *             If an unexpected error prevented the load manager from being started.
 */
@Override
public void start() throws PulsarServerException {
    try {
        // Register the brokers in zk list
        createZPathIfNotExists(zkClient, LoadManager.LOADBALANCE_BROKERS_ROOT);
        createZPathIfNotExists(zkClient, CETUS_COORDINATE_DATA_ROOT);

        String lookupServiceAddress = pulsar.getAdvertisedAddress() + ":" + conf.getWebServicePort();
        brokerZnodePath = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + lookupServiceAddress;
        cetusBrokerZnodePath = CETUS_COORDINATE_DATA_ROOT + "/" + lookupServiceAddress;
        final String timeAverageZPath = TIME_AVERAGE_BROKER_ZPATH + "/" + lookupServiceAddress;
        pulsar.writeCoordinateDataOnZookeeper();
        updateLocalBrokerData();
        try {
            ZkUtils.createFullPathOptimistic(zkClient, brokerZnodePath, localData.getJsonBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            ZkUtils.createFullPathOptimistic(zkClient, cetusBrokerZnodePath, pulsar.getCetusBrokerData().getJsonBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            long ownerZkSessionId = getBrokerZnodeOwner();
            if (ownerZkSessionId != 0 && ownerZkSessionId != zkClient.getSessionId()) {
                log.error("Broker znode - [{}] is own by different zookeeper-ssession {} ", brokerZnodePath,
                        ownerZkSessionId);
                throw new PulsarServerException(
                        "Broker-znode owned by different zk-session " + ownerZkSessionId);
            }
            // Node may already be created by another load manager: in this case update the data.
            zkClient.setData(brokerZnodePath, localData.getJsonBytes(), -1);
            zkClient.setData(cetusBrokerZnodePath, pulsar.getCetusBrokerData().getJsonBytes(), -1);
        } catch (Exception e) {
            // Catching exception here to print the right error message
            log.error("Unable to create znode - [{}] for load balance on zookeeper ", brokerZnodePath, e);
            throw e;
        }
        createZPathIfNotExists(zkClient, timeAverageZPath);
        zkClient.setData(timeAverageZPath, (new TimeAverageBrokerData()).getJsonBytes(), -1);
        updateAll();
        lastBundleDataUpdate = System.currentTimeMillis();
    } catch (Exception e) {
        log.error("Unable to create znode - [{}] for load balance on zookeeper ", brokerZnodePath, e);
        throw new PulsarServerException(e);
    }
}

/**
 * As any broker, stop the load manager.
 *
 * @throws PulsarServerException
 *             If an unexpected error occurred when attempting to stop the load manager.
 */
@Override
public void stop() throws PulsarServerException {
    if (availableActiveBrokers != null) {
        availableActiveBrokers.close();
    }

    if (brokerDataCache != null) {
        brokerDataCache.close();
        brokerDataCache.clear();
    }
    if(cetusBrokerDataCache != null) {
        cetusBrokerDataCache.close();
        cetusBrokerDataCache.clear();
    }
    scheduler.shutdown();
}

/**
 * As any broker, retrieve the namespace bundle stats and system resource usage to update data local to this broker.
 * @return
 */
@Override
public LocalBrokerData updateLocalBrokerData() {
    try {
        final SystemResourceUsage systemResourceUsage = LoadManagerShared.getSystemResourceUsage(brokerHostUsage);
        localData.update(systemResourceUsage, getBundleStats());
    } catch (Exception e) {
        log.warn("Error when attempting to update local broker data: {}", e);
    }
    return localData;
}

/**
 * As any broker, write the local broker data to ZooKeeper.
 */
@Override
public void writeBrokerDataOnZooKeeper() {
    try {
        updateLocalBrokerData();
        //if (needBrokerDataUpdate()) {
        localData.setLastUpdate(System.currentTimeMillis());
        List<String> bundlesToRemove = new ArrayList<String>();
        for (String b : pulsar.getCetusBrokerData().getBundleNetworkCoordinates().keySet()) {
            if (!localData.getBundles().contains(b))
                bundlesToRemove.add(b);
        }
        for (String bundleToRemove : bundlesToRemove) {
            log.info("Removing bundle {} from CetusLatencyMonitoringData.BundleNetworkCoordinates");
            pulsar.getCetusBrokerData().getBundleNetworkCoordinates().remove(bundleToRemove);
        }
        log.info("Writing bundles to ZooKeeper: {}" ,localData.getBundles());
        zkClient.setData(brokerZnodePath, localData.getJsonBytes(), -1);

        // Clear deltas.
        localData.getLastBundleGains().clear();
        localData.getLastBundleLosses().clear();

        // Update previous data.
        lastData.update(localData);
        //log.info("Writing broker data to Zookeeper");
        //}
    } catch (Exception e) {
        log.warn("Error writing broker data on ZooKeeper: {}", e);
    }
}


@Override
public Deserializer<LocalBrokerData> getLoadReportDeserializer() {
    return loadReportDeserializer;
}

/**
 * As the leader broker, write bundle data aggregated from all brokers to ZooKeeper.
 */
@Override
public void writeBundleDataOnZooKeeper() {
    updateBundleData();
    // Write the bundle data to ZooKeeper.
    for (Map.Entry<String, BundleData> entry : cetusLoadData.getLoadData().getBundleData().entrySet()) {
        final String bundle = entry.getKey();
        final BundleData data = entry.getValue();
        try {
            final String zooKeeperPath = getBundleDataZooKeeperPath(bundle);
            createZPathIfNotExists(zkClient, zooKeeperPath);
            zkClient.setData(zooKeeperPath, data.getJsonBytes(), -1);
        } catch (Exception e) {
            log.warn("Error when writing data for bundle {} to ZooKeeper: {}", bundle, e);
        }
    }
    // Write the time average broker data to ZooKeeper.
    for (Map.Entry<String, BrokerData> entry : cetusLoadData.getLoadData().getBrokerData().entrySet()) {
        final String broker = entry.getKey();
        final TimeAverageBrokerData data = entry.getValue().getTimeAverageData();
        try {
            final String zooKeeperPath = TIME_AVERAGE_BROKER_ZPATH + "/" + broker;
            createZPathIfNotExists(zkClient, zooKeeperPath);
            zkClient.setData(zooKeeperPath, data.getJsonBytes(), -1);
            if (log.isDebugEnabled()) {
                log.debug("Writing zookeeper report {}", data);
            }
        } catch (Exception e) {
            log.warn("Error when writing time average broker data for {} to ZooKeeper: {}", broker, e);
        }
    }
}

private void deleteBundleDataFromZookeeper(String bundle) {
    final String zooKeeperPath = getBundleDataZooKeeperPath(bundle);
    try {
        if (zkClient.exists(zooKeeperPath, null) != null) {
            zkClient.delete(zooKeeperPath, -1);
        }
    } catch (Exception e) {
        log.warn("Failed to delete bundle-data {} from zookeeper", bundle, e);
    }
}

private long getBrokerZnodeOwner() {
    try {
        Stat stat = new Stat();
        zkClient.getData(brokerZnodePath, false, stat);
        return stat.getEphemeralOwner();
    } catch (Exception e) {
        log.warn("Failed to get stat of {}", brokerZnodePath, e);
    }
    return 0;
}

private void refreshBrokerToFailureDomainMap() {
    if (!pulsar.getConfiguration().isFailureDomainsEnabled()) {
        return;
    }
    final String clusterDomainRootPath = pulsar.getConfigurationCache().CLUSTER_FAILURE_DOMAIN_ROOT;
    try {
        synchronized (brokerToFailureDomainMap) {
            Map<String, String> tempBrokerToFailureDomainMap = Maps.newHashMap();
            for (String domainName : pulsar.getConfigurationCache().failureDomainListCache().get()) {
                try {
                    Optional<FailureDomain> domain = pulsar.getConfigurationCache().failureDomainCache()
                        .get(clusterDomainRootPath + "/" + domainName);
                    if (domain.isPresent()) {
                        for (String broker : domain.get().brokers) {
                            tempBrokerToFailureDomainMap.put(broker, domainName);
                        }
                    }
                } catch (Exception e) {
                    log.warn("Failed to get domain {}", domainName, e);
                }
            }
            this.brokerToFailureDomainMap = tempBrokerToFailureDomainMap;
        }
        log.info("Cluster domain refreshed {}", brokerToFailureDomainMap);
    } catch (Exception e) {
        log.warn("Failed to get domain-list for cluster {}", e.getMessage());
    }
}

private String getCoordinateZPath(final String broker) {
    final String brokerZPath = "/cetus/coordinate-data/" + broker;
    return brokerZPath;
}

/**
 * 
 * @param initiatorBrokerData
 */
private void checkReassignmentForLatency(final CetusLatencyMonitoringData initiatorBrokerData) {
    // currentBrokerNc <--- initiatorBrokerData.getBrokerNC()

    /**
     * for topic in initiatorBrokerData.topics()
     *      currentLatency <-- calculateNetworkLatencies(currentBrokerNc, initiatorBrokerData[topic].producers, initiatorBrokerData[topic].consumers)
     *                          + calculateBrokerLatency(initiatorBrokerData.getBrokerId(), topic)
     *      candidates <-- findNearbyBrokers(topics, threshold)
     *      for candidate in candidates
     *             calculate new end to end latency as if topic was to be moved to candidate
     *             if new latency < currentLatency
     *                  migrate
     */

}

private void latencyStatsUpdated(final CetusLatencyMonitoringData nwCoordData) {
    checkReassignmentForLatency(nwCoordData);
}
}
