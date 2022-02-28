package org.colloh.flink.kudu.connector.internal.convertor;

import org.apache.flink.types.Row;
import org.apache.kudu.Schema;
import org.apache.kudu.client.RowResult;

/**
 * @fileName: RowResultRowConvertor.java
 * @description: RowResult转换Row
 * @author: by echo huang
 * @date: 2021/3/3 3:16 下午
 */
public class RowResultRowConvertor implements RowResultConvertor<Row> {
    @Override
    public Row convertor(RowResult row) {
        Schema schema = row.getColumnProjection();

        Row values = new Row(schema.getColumnCount());
        schema.getColumns().forEach(column -> {
            String name = column.getName();
            if (row.isNull(name)){
                return;
            }
            int pos = schema.getColumnIndex(name);
            values.setField(pos, row.getObject(name));
        });
        return values;

    }
}
