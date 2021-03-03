package org.colloh.flink.kudu.connector.table.lookup;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.colloh.flink.kudu.connector.internal.KuduFilterInfo;
import org.colloh.flink.kudu.connector.internal.KuduTableInfo;
import org.colloh.flink.kudu.connector.internal.convertor.RowResultConvertor;
import org.colloh.flink.kudu.connector.internal.metrics.IntegerGauge;
import org.colloh.flink.kudu.connector.internal.reader.KuduInputSplit;
import org.colloh.flink.kudu.connector.internal.reader.KuduReader;
import org.colloh.flink.kudu.connector.internal.reader.KuduReaderConfig;
import org.colloh.flink.kudu.connector.internal.reader.KuduReaderIterator;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @fileName: KuduLookupFunction.java
 * @description: kudu lookupFucntion基类
 * @author: by echo huang
 * @date: 2020/12/29 2:22 下午
 */
public abstract class BaseKuduLookupFunction<T> extends TableFunction<T> {
    private static final Logger LOG = LoggerFactory.getLogger(BaseKuduLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final KuduTableInfo tableInfo;
    private final KuduReaderConfig kuduReaderConfig;
    private final String[] keyNames;
    private final String[] projectedFields;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final RowResultConvertor<T> convertor;

    private transient Cache<T, List<T>> cache;
    private transient KuduReader<T> kuduReader;
    private transient Integer keyCount = 0;

    public BaseKuduLookupFunction(String[] keyNames, RowResultConvertor<T> convertor, KuduTableInfo tableInfo, KuduReaderConfig kuduReaderConfig, String[] projectedFields, KuduLookupOptions kuduLookupOptions) {
        this.tableInfo = tableInfo;
        this.convertor = convertor;
        this.projectedFields = projectedFields;
        this.keyNames = keyNames;
        this.kuduReaderConfig = kuduReaderConfig;
        this.cacheMaxSize = kuduLookupOptions.getCacheMaxSize();
        this.cacheExpireMs = kuduLookupOptions.getCacheExpireMs();
        this.maxRetryTimes = kuduLookupOptions.getMaxRetryTimes();
    }

    public abstract T buildCacheKey(Object... keys);

    /**
     * The invoke entry point of lookup function.
     *
     * @param keys keys
     */
    public void eval(Object... keys) {
        if (keys.length != keyNames.length) {
            throw new RuntimeException("lookUpKey和lookUpKeyVals长度不一致");
        }
        // 用于底层缓存用
        T keyRow = buildCacheKey(keys);
        if (this.cache != null) {
            ConcurrentMap<T, List<T>> cacheMap = this.cache.asMap();
            this.keyCount = cacheMap.size();
            List<T> cacheRows = this.cache.getIfPresent(keyRow);
            if (CollectionUtils.isNotEmpty(cacheRows)) {
                for (T cacheRow : cacheRows) {
                    collect(cacheRow);
                }
                return;
            }
        }

        for (int retry = 1; retry <= maxRetryTimes; retry++) {
            try {
                List<KuduFilterInfo> kuduFilterInfos = buildKuduFilterInfo(keys);
                this.kuduReader.setTableFilters(kuduFilterInfos);
                this.kuduReader.setTableProjections(ArrayUtils.isNotEmpty(projectedFields) ? Arrays.asList(projectedFields) : null);

                KuduInputSplit[] inputSplits = kuduReader.createInputSplits(1);
                ArrayList<T> rows = new ArrayList<>();
                for (KuduInputSplit inputSplit : inputSplits) {
                    KuduReaderIterator<T> scanner = kuduReader.scanner(inputSplit.getScanToken());
                    // 没有启用cache
                    if (cache == null) {
                        while (scanner.hasNext()) {
                            collect(scanner.next());
                        }
                    } else {
                        while (scanner.hasNext()) {
                            T row = scanner.next();
                            rows.add(row);
                            collect(row);
                        }
                        rows.trimToSize();
                    }
                }
                if (cache != null) {
                    cache.put(keyRow, rows);
                }
                break;
            } catch (Exception e) {
                LOG.error(String.format("Kudu scan error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of Kudu scan failed.", e);
                }
                try {
                    Thread.sleep(1000L * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }

    /**
     * build kuduFilterInfo
     *
     * @return
     */
    private List<KuduFilterInfo> buildKuduFilterInfo(Object... keyValS) {
        List<KuduFilterInfo> kuduFilterInfos = Lists.newArrayList();
        for (int i = 0; i < keyNames.length; i++) {
            KuduFilterInfo kuduFilterInfo = KuduFilterInfo.Builder.create(keyNames[i])
                    .equalTo(keyValS[i]).build();
            kuduFilterInfos.add(kuduFilterInfo);
        }
        return kuduFilterInfos;
    }


    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");
        try {
            super.open(context);
            context.getMetricGroup().addGroup("kudu.lookup").gauge("keys", new IntegerGauge(this.keyCount));
            this.kuduReader = new KuduReader<T>(this.tableInfo, this.kuduReaderConfig, this.convertor);
            // 构造缓存用于缓存kudu数据
            this.cache = this.cacheMaxSize == -1 || this.cacheExpireMs == -1 ? null : Caffeine.newBuilder()
                    .expireAfterWrite(this.cacheExpireMs, TimeUnit.MILLISECONDS)
                    .maximumSize(this.cacheMaxSize)
                    .build();
        } catch (Exception ioe) {
            LOG.error("Exception while creating connection to Kudu.", ioe);
            throw new RuntimeException("Cannot create connection to Kudu.", ioe);
        }
        LOG.info("end open.");
    }

    @Override
    public void close() {
        LOG.info("start close ...");
        if (null != this.kuduReader) {
            try {
                this.kuduReader.close();
                this.cache.cleanUp();
                // help gc
                this.cache = null;
                this.kuduReader = null;
            } catch (IOException e) {
                // ignore exception when close.
                LOG.warn("exception when close table", e);
            }
        }
        LOG.info("end close.");
    }
}
