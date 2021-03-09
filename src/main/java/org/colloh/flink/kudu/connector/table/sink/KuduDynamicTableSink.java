package org.colloh.flink.kudu.connector.table.sink;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.colloh.flink.kudu.connector.internal.KuduTableInfo;
import org.colloh.flink.kudu.connector.internal.writer.KuduWriterConfig;
import org.colloh.flink.kudu.connector.internal.writer.RowDataUpsertOperationMapper;

import java.util.Objects;

/**
 * @fileName: KuduDynamicTableSink.java
 * @description: 动态tableSink
 * @author: by echo huang
 * @date: 2021/3/1 4:47 下午
 */
public class KuduDynamicTableSink implements DynamicTableSink {
    private final KuduWriterConfig.Builder writerConfigBuilder;
    private final TableSchema flinkSchema;
    private final KuduTableInfo tableInfo;

    public KuduDynamicTableSink(KuduWriterConfig.Builder writerConfigBuilder, TableSchema flinkSchema, KuduTableInfo tableInfo) {
        this.writerConfigBuilder = writerConfigBuilder;
        this.flinkSchema = flinkSchema;
        this.tableInfo = tableInfo;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        this.validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).addContainedKind(RowKind.DELETE).addContainedKind(RowKind.UPDATE_AFTER).build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        Preconditions.checkState(ChangelogMode.insertOnly().equals(requestedMode) || this.tableInfo.getSchema().getPrimaryKeyColumnCount() != 0, "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        KuduSink<RowData> upsertKuduSink = new KuduSink<>(writerConfigBuilder.build(), tableInfo, new RowDataUpsertOperationMapper(flinkSchema));
        return SinkFunctionProvider.of(upsertKuduSink);
    }

    @Override
    public DynamicTableSink copy() {
        return new KuduDynamicTableSink(this.writerConfigBuilder, this.flinkSchema, this.tableInfo);
    }

    @Override
    public String asSummaryString() {
        return "kudu";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KuduDynamicTableSink that = (KuduDynamicTableSink) o;
        return Objects.equals(writerConfigBuilder, that.writerConfigBuilder) && Objects.equals(flinkSchema, that.flinkSchema) && Objects.equals(tableInfo, that.tableInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(writerConfigBuilder, flinkSchema, tableInfo);
    }
}
