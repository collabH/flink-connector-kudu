package org.colloh.flink.kudu.connector.table.lookup;

import org.colloh.flink.kudu.connector.internal.KuduTableInfo;
import org.colloh.flink.kudu.connector.internal.convertor.RowResultRowConvertor;
import org.colloh.flink.kudu.connector.internal.reader.KuduReaderConfig;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @fileName: KuduLookupFunction.java
 * @description: 支持kudu lookup function
 * @author: by echo huang
 * @date: 2020/12/29 2:22 下午
 */
public class RowLookupFunction extends BaseKuduLookupFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(RowLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private RowLookupFunction(String[] keyNames, KuduTableInfo tableInfo, KuduReaderConfig kuduReaderConfig, String[] projectedFields, KuduLookupOptions kuduLookupOptions) {
        super(keyNames, new RowResultRowConvertor(), tableInfo, kuduReaderConfig, projectedFields, kuduLookupOptions);
    }

    @Override
    public Row buildCacheKey(Object... keys) {
        return Row.of(keys);
    }

    public static class Builder {
        private KuduTableInfo tableInfo;
        private KuduReaderConfig kuduReaderConfig;
        private String[] keyNames;
        private String[] projectedFields;
        private KuduLookupOptions kuduLookupOptions;

        public static Builder options() {
            return new Builder();
        }

        public Builder tableInfo(KuduTableInfo tableInfo) {
            this.tableInfo = tableInfo;
            return this;
        }

        public Builder kuduReaderConfig(KuduReaderConfig kuduReaderConfig) {
            this.kuduReaderConfig = kuduReaderConfig;
            return this;
        }

        public Builder keyNames(String[] keyNames) {
            this.keyNames = keyNames;
            return this;
        }

        public Builder projectedFields(String[] projectedFields) {
            this.projectedFields = projectedFields;
            return this;
        }

        public Builder kuduLookupOptions(KuduLookupOptions kuduLookupOptions) {
            this.kuduLookupOptions = kuduLookupOptions;
            return this;
        }

        public RowLookupFunction build() {
            return new RowLookupFunction(keyNames, tableInfo, kuduReaderConfig, projectedFields, kuduLookupOptions);
        }
    }
}
