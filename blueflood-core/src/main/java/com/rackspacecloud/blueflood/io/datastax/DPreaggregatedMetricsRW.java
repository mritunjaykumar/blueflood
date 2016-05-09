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

package com.rackspacecloud.blueflood.io.datastax;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.outputs.formats.MetricData;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * This class deals with reading/writing metrics to the metrics_preaggregated_* column families
 * using Datastax driver
 */
public class DPreaggregatedMetricsRW extends DAbstractMetricsRW implements PreaggregatedRW {

    private static final Logger LOG = LoggerFactory.getLogger(DPreaggregatedMetricsRW.class);

    /**
     * Inserts a collection of metrics to the metrics_preaggregated_full column family
     *
     * @param metrics
     * @throws IOException
     */
    @Override
    public void insertMetrics(Collection<IMetric> metrics) throws IOException {
        insertMetrics(metrics, Granularity.FULL);
    }

    /**
     * Inserts a collection of rolled up metrics to the metrics_preaggregated_{granularity} column family.
     * Only our tests should call this method. Services should call either insertMetrics(Collection metrics)
     * or insertRollups()
     *
     * @param metrics
     * @throws IOException
     */
    @VisibleForTesting
    @Override
    public void insertMetrics(Collection<IMetric> metrics,
                              Granularity granularity) throws IOException {
        Timer.Context ctx = Instrumentation.getWriteTimerContext(
                CassandraModel.getPreaggregatedColumnFamilyName(granularity));
        try {
            Map<ResultSetFuture, Locator> futureLocatorMap = new HashMap<ResultSetFuture, Locator>();
            Multimap<Locator, IMetric> map = asMultimap(metrics);
            for (Locator locator : map.keySet()) {
                for (IMetric metric : map.get(locator)) {
                    RollupType rollupType = metric.getRollupType();

                    // lookup the right io object
                    DAbstractMetricIO io = rollupTypeToIO.get(rollupType);
                    if ( io == null ) {
                        throw new InvalidDataException(
                                String.format("insertMetrics(locator=%s, granularity=%s): unsupported preaggregated rollupType=%s",
                                        locator, granularity, rollupType.name()));
                    }

                    if (!(metric.getMetricValue() instanceof Rollup)) {
                        throw new InvalidDataException(
                                String.format("insertMetrics(locator=%s, granularity=%s): metric value %s is not type Rollup",
                                        locator, granularity, metric.getMetricValue().getClass().getSimpleName())
                        );
                    }
                    ResultSetFuture future = io.putAsync(locator, metric.getCollectionTime(),
                            (Rollup) metric.getMetricValue(),
                            granularity, metric.getTtlInSeconds());
                    futureLocatorMap.put(future, locator);

                    if ( !isLocatorCurrent(locator) ) {
                        locatorIO.insertLocator(locator);
                        setLocatorCurrent(locator);
                    }  else {
                        LOG.debug("insertMetrics(): not inserting locator " + locator);
                    }
                }
            }

            for (ResultSetFuture future : futureLocatorMap.keySet()) {
                try {
                    future.getUninterruptibly().all();
                } catch (Exception ex) {
                    Instrumentation.markWriteError();
                    LOG.error(String.format("error writing preaggregated metric for locator %s, granularity %s",
                            futureLocatorMap.get(future), granularity), ex);
                }
            }
        } finally {
            ctx.stop();
        }
    }

    /**
     * Fetches {@link com.rackspacecloud.blueflood.outputs.formats.MetricData} objects for the
     * specified list of {@link com.rackspacecloud.blueflood.types.Locator} and
     * {@link com.rackspacecloud.blueflood.types.Range} from the specified column family
     *
     * @param locators
     * @param range
     * @param granularity
     * @return
     */
    @Override
    public Map<Locator, MetricData> getDatapointsForRange(List<Locator> locators,
                                                          Range range,
                                                          Granularity granularity) throws IOException {

        Timer.Context ctx = Instrumentation.getReadTimerContext(
                granularity.name() );

        try {

            String columnFamily = CassandraModel.getPreaggregatedColumnFamilyName(granularity);

            return getDatapointsForRange( locators, range, columnFamily, granularity );

        } finally {
            ctx.stop();
        }
    }
}
