/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.metric;

import azkaban.utils.Props;
import com.google.common.collect.ImmutableList;
import com.relateiq.statsd.impl.DatadogClient;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class DatadogMetricEmitter implements IMetricEmitter {
    private static final String DATADOG_PREFIX = "azkaban.metric.datadog.prefix";
    private static final String DATADOG_ENVIRONMENT = "azkaban.metric.datadog.environment";
    private static final String DATADOG_API_KEY = "azkaban.metric.datadog.api.key";

    private String statsApiKey;
    private String statsPrefix;
    private String statsEnvironment;

    private static DatadogClient datadogClient;

    public DatadogMetricEmitter(Props azkProps) {
        statsPrefix = azkProps.getString(DATADOG_PREFIX, "azkaban.metrics");
        statsApiKey = azkProps.getString(DATADOG_API_KEY);
        statsEnvironment = azkProps.getString(DATADOG_ENVIRONMENT, "dev");
        datadogClient = new DatadogClient(statsApiKey);
    }

    @Override
    public void reportMetric(final IMetric<?> metric) throws MetricException {

        List<DatadogClient.DDMetric> series;
        // Either a Map or assume Integer (from default impl)
        if (metric.getValue() instanceof Map) {
            Map<String, Integer> metricMap = (Map<String, Integer>) metric.getValue();

            series = metricMap.entrySet().stream().map(metricEntry -> DatadogClient.DDMetric.builder()
                    .metric(statsPrefix + "." + metric.getName())
                    .points(ImmutableList.of(
                            ImmutableList.of(Math.toIntExact(Instant.now().getEpochSecond()), metricEntry.getValue())
                    ))
                    .type(DatadogClient.DDMetricType.gauge)
                    .tags(ImmutableList.of("environment:" + statsEnvironment, "project:" + metricEntry.getKey()))
                    .build()).collect(Collectors.toList());

        } else {
            series = ImmutableList.of(
                    DatadogClient.DDMetric.builder()
                            .metric(statsPrefix + "." + metric.getName())
                            .points(ImmutableList.of(
                                    ImmutableList.of(Math.toIntExact(Instant.now().getEpochSecond()), (Integer) metric.getValue())
                            ))
                            .type(DatadogClient.DDMetricType.gauge)
                            .tags(ImmutableList.of("environment:" + statsEnvironment))
                            .build());
        }

        datadogClient.report(DatadogClient.DDPayload.builder()
                .series(series)
                .build());
    }

    @Override
    public void purgeAllData() throws MetricException {

    }
}
