/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.colloh.flink.kudu.connector.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.colloh.flink.kudu.connector.internal.KuduTableInfo;
import org.colloh.flink.kudu.connector.internal.reader.KuduReaderConfig;
import org.colloh.flink.kudu.connector.internal.utils.KuduTableUtils;
import org.colloh.flink.kudu.connector.internal.writer.KuduWriterConfig;
import org.colloh.flink.kudu.connector.table.lookup.KuduLookupOptions;
import org.colloh.flink.kudu.connector.table.sink.KuduTableSink;
import org.colloh.flink.kudu.connector.table.source.KuduTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.EXPR;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class KuduTableFactory implements TableSourceFactory<Row>, TableSinkFactory<Tuple2<Boolean, Row>> {

    public static final String KUDU_TABLE = "kudu.table";
    public static final String KUDU_MASTERS = "kudu.masters";
    public static final String KUDU_HASH_COLS = "kudu.hash-columns";
    /**
     * hash分区bucket个数
     */
    public static final String KUDU_HASH_PARTITION_NUMS = "kudu.hash-partition-nums";
    /**
     * range分区规则，rangeKey#leftValue,RightValue:rangeKey#leftValue1,RightValue1
     */
    public static final String KUDU_RANGE_PARTITION_RULE = "kudu.range-partition-rule";
    public static final String KUDU_PRIMARY_KEY_COLS = "kudu.primary-key-columns";
    public static final String KUDU_REPLICAS = "kudu.replicas";
    public static final String KUDU_SCAN_ROW_SIZE = "kudu.scan.row-size";
    public static final String KUDU = "kudu";

    /**
     * lookup缓存最大行数
     */
    public static final String KUDU_LOOKUP_CACHE_MAX_ROWS = "kudu.lookup.cache.max-rows";
    /**
     * lookup缓存过期时间
     */
    public static final String KUDU_LOOKUP_CACHE_TTL = "kudu.lookup.cache.ttl";
    /**
     * kudu连接重试次数
     */
    public static final String KUDU_LOOKUP_MAX_RETRIES = "kudu.lookup.max-retries";


    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, KUDU);
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        properties.add(KUDU_TABLE);
        properties.add(KUDU_MASTERS);
        properties.add(KUDU_HASH_COLS);
        properties.add(KUDU_PRIMARY_KEY_COLS);
        properties.add(KUDU_HASH_PARTITION_NUMS);
        properties.add(KUDU_RANGE_PARTITION_RULE);
        properties.add(KUDU_SCAN_ROW_SIZE);
        properties.add(KUDU_REPLICAS);

        //lookup
        properties.add(KUDU_LOOKUP_CACHE_MAX_ROWS);
        properties.add(KUDU_LOOKUP_CACHE_TTL);
        properties.add(KUDU_LOOKUP_MAX_RETRIES);
        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        properties.add(SCHEMA + ".#." + SCHEMA_FROM);
        // computed column
        properties.add(SCHEMA + ".#." + EXPR);

        // time attributes
        properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

        // watermark
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_ROWTIME);
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_EXPR);
        properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_DATA_TYPE);
        return properties;
    }

    private DescriptorProperties validateTable(CatalogTable table) {
        Map<String, String> properties = table.toProperties();
        checkNotNull(properties.get(KUDU_MASTERS), "Missing required property " + KUDU_MASTERS);

        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        new SchemaValidator(true, false, false).validate(descriptorProperties);
        return descriptorProperties;
    }

    @Override
    public KuduTableSource createTableSource(ObjectPath tablePath, CatalogTable table) {
        validateTable(table);
        String tableName = table.toProperties().getOrDefault(KUDU_TABLE, tablePath.getObjectName());
        return createTableSource(tableName, table.getSchema(), table.getOptions());
    }

    private KuduTableSource createTableSource(String tableName, TableSchema schema, Map<String, String> props) {
        String masterAddresses = props.get(KUDU_MASTERS);
        // 默认为0，表示拉取最大的数据量
        int scanRowSize = Integer.parseInt(props.getOrDefault(KUDU_SCAN_ROW_SIZE, "0"));
        long kuduCacheMaxRows = Long.parseLong(props.getOrDefault(KUDU_LOOKUP_CACHE_MAX_ROWS, "-1"));
        long kuduCacheTtl = Long.parseLong(props.getOrDefault(KUDU_LOOKUP_CACHE_TTL, "-1"));
        // kudu重试次数，默认为3次
        int kuduMaxReties = Integer.parseInt(props.getOrDefault(KUDU_LOOKUP_MAX_RETRIES, "3"));

        // 构造kudu lookup options
        KuduLookupOptions kuduLookupOptions = KuduLookupOptions.Builder.options().withCacheMaxSize(kuduCacheMaxRows)
                .withCacheExpireMs(kuduCacheTtl)
                .withMaxRetryTimes(kuduMaxReties)
                .build();


        TableSchema physicalSchema = KuduTableUtils.getSchemaWithSqlTimestamp(schema);
        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(tableName, schema, props);

        KuduReaderConfig.Builder configBuilder = KuduReaderConfig.Builder
                .setMasters(masterAddresses)
                .setRowLimit(scanRowSize);

        return new KuduTableSource(configBuilder, tableInfo, physicalSchema, null, null, kuduLookupOptions);
    }

    @Override
    public KuduTableSink createTableSink(ObjectPath tablePath, CatalogTable table) {
        validateTable(table);
        String tableName = table.toProperties().getOrDefault(KUDU_TABLE, tablePath.getObjectName());
        return createTableSink(tableName, table.getSchema(), table.getOptions());
    }

    private KuduTableSink createTableSink(String tableName, TableSchema schema, Map<String, String> props) {
        String masterAddresses = props.get(KUDU_MASTERS);
        TableSchema physicalSchema = KuduTableUtils.getSchemaWithSqlTimestamp(schema);
        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(tableName, schema, props);

        KuduWriterConfig.Builder configBuilder = KuduWriterConfig.Builder
                .setMasters(masterAddresses);

        return new KuduTableSink(configBuilder, tableInfo, physicalSchema);
    }
}
