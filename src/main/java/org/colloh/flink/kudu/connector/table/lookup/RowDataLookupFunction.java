package org.colloh.flink.kudu.connector.table.lookup;

import org.colloh.flink.kudu.connector.internal.KuduTableInfo;
import org.colloh.flink.kudu.connector.internal.convertor.RowResultRowDataConvertor;
import org.colloh.flink.kudu.connector.internal.reader.KuduReaderConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

/**
 * @fileName: KuduLookupFunction.java
 * @description: 支持kudu lookup function
 * @author: by echo huang
 * @date: 2020/12/29 2:22 下午
 */
public class RowDataLookupFunction extends BaseKuduLookupFunction<RowData> {
    private static final long serialVersionUID = 1L;


    private RowDataLookupFunction(String[] keyNames, KuduTableInfo tableInfo, KuduReaderConfig kuduReaderConfig, String[] projectedFields, KuduLookupOptions kuduLookupOptions) {
        super(keyNames, new RowResultRowDataConvertor(), tableInfo, kuduReaderConfig, projectedFields, kuduLookupOptions);
    }

    @Override
    public RowData buildCacheKey(Object... keys) {
        return GenericRowData.of(keys);
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

        public RowDataLookupFunction build() {
            return new RowDataLookupFunction(keyNames, tableInfo, kuduReaderConfig, projectedFields, kuduLookupOptions);
        }
    }
}
