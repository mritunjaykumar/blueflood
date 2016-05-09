/*
 * Copyright (c) 2016 Rackspace.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rackspacecloud.blueflood.io;

import com.google.common.collect.Table;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.exceptions.CacheException;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * This class uses the driver specific {@link EnumReaderIO} to
 * read/write enum metrics from Cassandra and convert them to
 * {@link com.rackspacecloud.blueflood.outputs.formats.MetricData}
 * objects that are used by services such as Rollup, Ingest and Query
 */
public class EnumMetricData {

    private static final Logger LOG = LoggerFactory.getLogger(EnumMetricData.class);

    private static final Configuration configuration = Configuration.getInstance();
    private static final ExecutorService taskExecutor = new ThreadPoolBuilder()
                                            .withUnboundedQueue()
                                            .withCorePoolSize(configuration.getIntegerProperty(CoreConfig.ENUM_READ_THREADS))
                                            .withMaxPoolSize(configuration.getIntegerProperty(CoreConfig.ENUM_READ_THREADS))
                                            .withName("Read Enum Values")
                                            .build();
    private static final MetadataCache metadataCache = MetadataCache.getInstance();

    private EnumReaderIO enumReader;

    /**
     * Constructor. Takes an instance or EnumReader, which would be
     * driver specific
     *
     * @param reader
     */
    public EnumMetricData(EnumReaderIO reader) {
        enumReader = reader;
    }

    /**
     * This method fetches enum metrics for a particular range, granularity, and {@link com.rackspacecloud.blueflood.types.Locator}.
     * The result is organized in a {@link com.rackspacecloud.blueflood.outputs.formats.MetricData} object.
     *
     * @param locator
     * @param range
     * @param gran
     * @return
     */
    public MetricData getEnumMetricDataForRange(final Locator locator, final Range range, final Granularity gran) {
        Map<Locator, MetricData> metricDataMap = getEnumMetricDataForRangeForLocatorList(new ArrayList<Locator>(){{add (locator); }}, range, gran);
        return metricDataMap.get(locator);
    }

    /**
     * This method fetches all enum metrics for a particular range and granularity by their {@link com.rackspacecloud.blueflood.types.Locator}.
     * The result is organized in a map of Locator -> {@link com.rackspacecloud.blueflood.outputs.formats.MetricData}.
     *
     * @param locators
     * @param range
     * @param gran
     * @return
     */
    public Map<Locator, MetricData> getEnumMetricDataForRangeForLocatorList(final List<Locator> locators, final Range range, final Granularity gran) {
        String columnFamily = CassandraModel.getColumnFamily(BluefloodEnumRollup.class, gran).getName();
        return getEnumMetricDataForRangeForLocatorList(locators, range, columnFamily);
    }

    /**
     * Reads from both the metrics_enum and metrics_preaggregated_{granularity} column families,
     * in parallel, and join the data to construct {@link com.rackspacecloud.blueflood.outputs.formats.MetricData}
     * objects
     *
     * @param locators
     * @param range
     * @param columnFamily
     * @return
     */
    private  Map<Locator, MetricData> getEnumMetricDataForRangeForLocatorList(final List<Locator> locators, final Range range, final String columnFamily) {

        if (range.getStart() > range.getStop()) {
            throw new IllegalArgumentException(String.format("invalid range: ", range.toString()));
        }

        Future<Table<Locator, Long, String>> enumHashValuesFuture = taskExecutor.submit(new Callable() {
            @Override
            public Table<Locator, Long, String> call() throws Exception {
                return enumReader.getEnumHashValuesForLocators(locators);
            }

        });

        Future<Table<Locator, Long, Object>> enumHashRollupFuture = taskExecutor.submit(new Callable() {
            @Override
            public Table<Locator, Long, Object> call() throws Exception {
                return enumReader.getEnumRollupsForLocators(locators, columnFamily, range);
            }
        });

        Map<Locator, MetricData> resultMap = new HashMap<Locator, MetricData>();
        try {
            Table<Locator, Long, String> enumHashValues =  enumHashValuesFuture.get();
            Table<Locator, Long, Object> enumHashRollup = enumHashRollupFuture.get();

            for (Locator locator : locators) {
                populateEnumValueToCountMap(enumHashRollup.row(locator), enumHashValues.row(locator));
                Points points = convertToPoints(enumHashRollup.row(locator));
                MetricData metricData = new MetricData(points, getUnitString(locator), MetricData.Type.ENUM);
                resultMap.put(locator, metricData);
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while reading Enum metrics for locators " + locators, e);
        } catch (ExecutionException e) {
            LOG.error("Execution error while reading Enum metrics for locators " + locators, e);
        }
        return resultMap;
    }

    /**
     * What we store in Cassandra is the serialized enum rollup object. We only serialize the enum hash code to its count
     * in Cassandra. But {@link com.rackspacecloud.blueflood.types.BluefloodEnumRollup} object has an internal
     * {@link java.util.Map} of enum string to its count. This method is to populate that Map by cross referencing
     * the enum hash code with its enum string, read from metrics_enum column family.
     *
     * @param enumRollupMap      a map of timestamp -> {@link com.rackspacecloud.blueflood.types.BluefloodEnumRollup}
     * @param enumHashValuesMap  a map of enum hash code -> enum string value
     */
    private void populateEnumValueToCountMap(Map<Long, Object> enumRollupMap, Map<Long, String> enumHashValuesMap) {
        for ( Object o : enumRollupMap.values() ) {
            BluefloodEnumRollup enumRollup = (BluefloodEnumRollup) o;
            for ( Map.Entry<Long, Long> hashCount: enumRollup.getHashedEnumValuesWithCounts().entrySet()){
                Long hash = hashCount.getKey();
                enumRollup.getStringEnumValuesWithCounts().put(enumHashValuesMap.get(hash), hashCount.getValue());
            }
        }
    }

    /**
     * Converts the map of timestamp -> {@link com.rackspacecloud.blueflood.types.BluefloodEnumRollup} to
     * {@link Points} object
     *
     * @param enumHashToRollupMap
     * @return
     */
    private Points convertToPoints(final Map<Long, Object> enumHashToRollupMap) {
        Points enumRollupPoints =  new Points();
        for (Map.Entry<Long, Object> enumRollup : enumHashToRollupMap.entrySet() ) {
            enumRollupPoints.add(new Points.Point(enumRollup.getKey(), enumRollup.getValue()));
        }
        return enumRollupPoints;
    }

    // TODO: can this move to MetadataCache
    private String getUnitString(Locator locator) {
        String unitString = Util.UNKNOWN;
        // Only grab units from cassandra, if we have to
        if (!Util.shouldUseESForUnits()) {
            try {
                unitString = metadataCache.get(locator, MetricMetadata.UNIT.name().toLowerCase(), String.class);
            } catch (CacheException ex) {
                LOG.warn("Cache exception reading unitString from MetadataCache: ", ex);
            }
            if (unitString == null) {
                unitString = Util.UNKNOWN;
            }
        }
        return unitString;
    }

}
