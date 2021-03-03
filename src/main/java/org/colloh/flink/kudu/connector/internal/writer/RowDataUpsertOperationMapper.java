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
package org.colloh.flink.kudu.connector.internal.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;

import java.util.Optional;

@Internal
public class RowDataUpsertOperationMapper extends AbstractSingleOperationMapper<RowData> {

    public RowDataUpsertOperationMapper(String[] columnNames) {
        super(columnNames);
    }

    @Override
    public Object getField(RowData input, int i) {

        GenericRowData f1 = (GenericRowData) input;
        return f1.getField(i);
    }

    @Override
    public Optional<Operation> createBaseOperation(RowData input, KuduTable table) {
        return Optional.of(input.getRowKind().equals(RowKind.INSERT)
                || input.getRowKind().equals(RowKind.UPDATE_AFTER) ? table.newUpsert() : table.newDelete());
    }
}
