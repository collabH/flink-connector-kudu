package org.colloh.flink.kudu.connector.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.colloh.flink.kudu.connector.internal.KuduTableInfo;
import org.colloh.flink.kudu.connector.internal.reader.KuduReaderConfig;
import org.colloh.flink.kudu.connector.internal.utils.KuduTableUtils;
import org.colloh.flink.kudu.connector.internal.writer.KuduWriterConfig;
import org.colloh.flink.kudu.connector.table.lookup.KuduLookupOptions;
import org.colloh.flink.kudu.connector.table.sink.KuduDynamicTableSink;
import org.colloh.flink.kudu.connector.table.source.KuduDynamicTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.kudu.shaded.com.google.common.collect.Sets;

import java.util.Set;

/**
 * @fileName: KuduDynamicTableSourceFactory.java
 * @description: kudu动态table工厂
 * @author: by echo huang
 * @date: 2021/3/1 3:46 下午
 */
public class KuduDynamicTableSourceSinkFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    public static final String IDENTIFIER = "kudu";
    public static final ConfigOption<String> KUDU_TABLE = ConfigOptions
            .key("kudu.table")
            .stringType()
            .noDefaultValue()
            .withDescription("kudu's table name");

    public static final ConfigOption<String> KUDU_MASTERS =
            ConfigOptions
                    .key("kudu.masters")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu's master server address");


    public static final ConfigOption<String> KUDU_HASH_COLS =
            ConfigOptions
                    .key("kudu.hash-columns")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu's hash columns");

    public static final ConfigOption<Integer> KUDU_REPLICAS =
            ConfigOptions
                    .key("kudu.replicas")
                    .intType()
                    .defaultValue(3)
                    .withDescription("kudu's replica nums");
    /**
     * hash分区bucket个数
     */
    public static final ConfigOption<Integer> KUDU_HASH_PARTITION_NUMS =
            ConfigOptions
                    .key("kudu.hash-partition-nums")
                    .intType()
                    .defaultValue(KUDU_REPLICAS.defaultValue() * 2)
                    .withDescription("kudu's hash partition bucket nums,defaultValue is 2 * replica nums");
    /**
     * range分区规则，rangeKey#leftValue,RightValue:rangeKey#leftValue1,RightValue1
     */
    public static final ConfigOption<String> KUDU_RANGE_PARTITION_RULE =
            ConfigOptions
                    .key("kudu.range-partition-rule")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu's range partition rule,rangeKey must be primary key,eg.id#10,20:id#20,30 equals 10<=id<20,20<=id<30");

    public static final ConfigOption<String> KUDU_PRIMARY_KEY_COLS =
            ConfigOptions
                    .key("kudu.primary-key-columns")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu's primary key,primary key must be ordered");


    public static final ConfigOption<Integer> KUDU_SCAN_ROW_SIZE =
            ConfigOptions
                    .key("kudu.scan.row-size")
                    .intType()
                    .defaultValue(0)
                    .withDescription("kudu's scan row size");

    public static final String KUDU = "kudu";

    /**
     * lookup缓存最大行数
     */
    public static final ConfigOption<Long> KUDU_LOOKUP_CACHE_MAX_ROWS =
            ConfigOptions
                    .key("kudu.lookup.cache.max-rows")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("the max number of rows of lookup cache, over this value, the oldest rows will " +
                            "be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is " +
                            "specified. Cache is not enabled as default.");
    /**
     * lookup缓存过期时间
     */
    public static final ConfigOption<Long> KUDU_LOOKUP_CACHE_TTL =
            ConfigOptions
                    .key("kudu.lookup.cache.ttl")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("the cache time to live.");

    /**
     * kudu连接重试次数
     */
    public static final ConfigOption<Integer> KUDU_LOOKUP_MAX_RETRIES =
            ConfigOptions
                    .key("kudu.lookup.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the max retry times if lookup database failed.");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        ReadableConfig config = getReadableConfig(context);
        String masterAddresses = config.get(KUDU_MASTERS);
        String tableName = config.get(KUDU_TABLE);
        TableSchema schema = context.getCatalogTable().getSchema();
        TableSchema physicalSchema = KuduTableUtils.getSchemaWithSqlTimestamp(schema);
        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(tableName, schema, context.getCatalogTable().toProperties());
        KuduWriterConfig.Builder configBuilder = KuduWriterConfig.Builder
                .setMasters(masterAddresses);
        return new KuduDynamicTableSink(configBuilder, physicalSchema, tableInfo);
    }

    /**
     * 获取readableConfig
     *
     * @param context 上下文
     * @return {@link ReadableConfig}
     */
    private ReadableConfig getReadableConfig(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        return helper.getOptions();
    }


    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        ReadableConfig config = getReadableConfig(context);
        String masterAddresses = config.get(KUDU_MASTERS);

        int scanRowSize = config.get(KUDU_SCAN_ROW_SIZE);
        long kuduCacheMaxRows = config.get(KUDU_LOOKUP_CACHE_MAX_ROWS);
        long kuduCacheTtl = config.get(KUDU_LOOKUP_CACHE_TTL);
        int kuduMaxReties = config.get(KUDU_LOOKUP_MAX_RETRIES);

        // 构造kudu lookup options
        KuduLookupOptions kuduLookupOptions = KuduLookupOptions.Builder.options().withCacheMaxSize(kuduCacheMaxRows)
                .withCacheExpireMs(kuduCacheTtl)
                .withMaxRetryTimes(kuduMaxReties)
                .build();

        TableSchema schema = context.getCatalogTable().getSchema();
        TableSchema physicalSchema = KuduTableUtils.getSchemaWithSqlTimestamp(schema);
        KuduTableInfo tableInfo = KuduTableUtils.createTableInfo(config.get(KUDU_TABLE), schema, context.getCatalogTable().toProperties());

        KuduReaderConfig.Builder configBuilder = KuduReaderConfig.Builder
                .setMasters(masterAddresses)
                .setRowLimit(scanRowSize);
        return new KuduDynamicTableSource(configBuilder, tableInfo, physicalSchema, null, physicalSchema.getFieldNames(), kuduLookupOptions);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Sets.newHashSet(KUDU_TABLE, KUDU_MASTERS);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Sets.newHashSet(KUDU_HASH_COLS, KUDU_HASH_PARTITION_NUMS, KUDU_RANGE_PARTITION_RULE,
                KUDU_PRIMARY_KEY_COLS, KUDU_SCAN_ROW_SIZE, KUDU_REPLICAS,
                //lookup
                KUDU_LOOKUP_CACHE_MAX_ROWS, KUDU_LOOKUP_CACHE_TTL, KUDU_LOOKUP_MAX_RETRIES);
    }
}
