package org.colloh.flink.kudu.connector.internal.convertor;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.RowResult;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * @fileName: RowResultRowConvertor.java
 * @description: RowResult转RowData
 * @author: by echo huang
 * @date: 2021/3/3 3:16 下午
 */
public class RowResultRowDataConvertor implements RowResultConvertor<RowData> {
    @Override
    public RowData convertor(RowResult row) {
        Schema schema = row.getColumnProjection();
        GenericRowData values = new GenericRowData(schema.getColumnCount());
        schema.getColumns().forEach(column -> {
            String name = column.getName();
            Type type = column.getType();
            int pos = schema.getColumnIndex(name);
            if (Objects.isNull(type)) {
                throw new IllegalArgumentException("columnName:" + name);
            }
            if (row.isNull(name)) {
                return;
            }
            switch (type) {
                case DECIMAL:
                    BigDecimal decimal = row.getDecimal(name);
                    values.setField(pos, DecimalData.fromBigDecimal(decimal, decimal.precision(), decimal.scale()));
                    break;
                case UNIXTIME_MICROS:
                    values.setField(pos, TimestampData.fromTimestamp(row.getTimestamp(name)));
                    break;
                case DOUBLE:
                    values.setField(pos, row.getDouble(name));
                    break;
                case STRING:
                    Object value = row.getObject(name);
                    values.setField(pos, StringData.fromString(Objects.nonNull(value) ? value.toString() : ""));
                    break;
                case BINARY:
                    values.setField(pos, row.getBinary(name));
                    break;
                case FLOAT:
                    values.setField(pos, row.getFloat(name));
                    break;
                case INT64:
                    values.setField(pos, row.getLong(name));
                    break;
                case INT32:
                case INT16:
                case INT8:
                    values.setField(pos, row.getInt(name));
                    break;
                case BOOL:
                    values.setField(pos, row.getBoolean(name));
                    break;
                default:
                    throw new IllegalArgumentException("columnName:" + name + ",type:" + type.getName() + "不支持!");
            }
        });
        return values;
    }
}
