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

package org.colloh.flink.kudu.connector.table.source;

import org.colloh.flink.kudu.connector.format.KuduRowDataInputFormat;
import org.colloh.flink.kudu.connector.internal.KuduFilterInfo;
import org.colloh.flink.kudu.connector.internal.KuduTableInfo;
import org.colloh.flink.kudu.connector.internal.convertor.RowResultRowDataConvertor;
import org.colloh.flink.kudu.connector.internal.reader.KuduReaderConfig;
import org.colloh.flink.kudu.connector.table.lookup.RowDataLookupFunction;
import org.colloh.flink.kudu.connector.table.lookup.KuduLookupOptions;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class KuduDynamicTableSource implements ScanTableSource, SupportsProjectionPushDown,
        SupportsLimitPushDown, LookupTableSource {

    private static final Logger LOG = LoggerFactory.getLogger(KuduDynamicTableSource.class);

    private final KuduReaderConfig.Builder configBuilder;
    private final KuduTableInfo tableInfo;
    private TableSchema physicalSchema;
    private final String[] projectedFields;
    // predicate expression to apply
    @Nullable
    private final List<KuduFilterInfo> predicates;
    private boolean isFilterPushedDown;
    private final KuduLookupOptions kuduLookupOptions;

    private final KuduRowDataInputFormat kuduRowDataInputFormat;

    public KuduDynamicTableSource(KuduReaderConfig.Builder configBuilder, KuduTableInfo tableInfo,
                                  TableSchema physicalSchema, List<KuduFilterInfo> predicates, String[] projectedFields, KuduLookupOptions kuduLookupOptions) {
        this.configBuilder = configBuilder;
        this.tableInfo = tableInfo;
        this.physicalSchema = physicalSchema;
        this.predicates = predicates;
        this.projectedFields = projectedFields;
        if (predicates != null && predicates.size() != 0) {
            this.isFilterPushedDown = true;
        }
        this.kuduRowDataInputFormat = new KuduRowDataInputFormat(configBuilder.build(), new RowResultRowDataConvertor(), tableInfo,
                predicates == null ? Collections.emptyList() : predicates,
                projectedFields == null ? null : Lists.newArrayList(projectedFields));
        this.kuduLookupOptions = kuduLookupOptions;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        int keysLen = context.getKeys().length;
        String[] keyNames = new String[keysLen];
        for (int i = 0; i < keyNames.length; ++i) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = this.physicalSchema.getFieldNames()[innerKeyArr[0]];
        }
        RowDataLookupFunction rowDataLookupFunction = RowDataLookupFunction.Builder.options()
                .keyNames(keyNames)
                .kuduReaderConfig(configBuilder.build())
                .projectedFields(projectedFields)
                .tableInfo(tableInfo)
                .kuduLookupOptions(kuduLookupOptions)
                .build();
        return TableFunctionProvider.of(rowDataLookupFunction);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        KuduRowDataInputFormat inputFormat = new KuduRowDataInputFormat(configBuilder.build(), new RowResultRowDataConvertor(), tableInfo,
                predicates == null ? Collections.emptyList() : predicates,
                projectedFields == null ? null : Lists.newArrayList(projectedFields));
        return InputFormatProvider.of(inputFormat);
    }

    @Override
    public DynamicTableSource copy() {
        return new KuduDynamicTableSource(this.configBuilder, this.tableInfo, this.physicalSchema, this.predicates, this.projectedFields, this.kuduLookupOptions);
    }

    @Override
    public String asSummaryString() {
        return "kudu";
    }

    @Override
    public boolean supportsNestedProjection() {
        //  planner doesn't support nested projection push down yet.
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.physicalSchema = TableSchemaUtils.projectSchema(this.physicalSchema, projectedFields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof KuduDynamicTableSource)) {
            return false;
        } else {
            KuduDynamicTableSource that = (KuduDynamicTableSource) o;
            return isFilterPushedDown == that.isFilterPushedDown && Objects.equals(configBuilder, that.configBuilder) && Objects.equals(tableInfo, that.tableInfo) && Objects.equals(physicalSchema, that.physicalSchema) && Arrays.equals(projectedFields, that.projectedFields) && Objects.equals(predicates, that.predicates) && Objects.equals(kuduLookupOptions, that.kuduLookupOptions) && Objects.equals(kuduRowDataInputFormat, that.kuduRowDataInputFormat);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(configBuilder, tableInfo, physicalSchema, predicates, isFilterPushedDown, kuduLookupOptions, kuduRowDataInputFormat);
    }


    @Override
    public void applyLimit(long limit) {
        configBuilder.setRowLimit((int) limit);
    }
}
