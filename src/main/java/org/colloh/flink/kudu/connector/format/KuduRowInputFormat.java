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
package org.colloh.flink.kudu.connector.format;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.colloh.flink.kudu.connector.internal.KuduFilterInfo;
import org.colloh.flink.kudu.connector.internal.KuduTableInfo;
import org.colloh.flink.kudu.connector.internal.convertor.RowResultConvertor;
import org.colloh.flink.kudu.connector.internal.reader.KuduReaderConfig;
import org.colloh.flink.kudu.connector.table.catalog.KuduCatalog;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Input format for reading the contents of a Kudu table (defined by the provided {@link KuduTableInfo}) in both batch
 * and stream programs. Rows of the Kudu table are mapped to {@link Row} instances that can converted to other data
 * types by the user later if necessary.
 *
 * <p> For programmatic access to the schema of the input rows users can use the {@link KuduCatalog}
 * or overwrite the column order manually by providing a list of projected column names.
 */
@PublicEvolving
public class KuduRowInputFormat extends BaseKuduInputFormat<Row> {

    public KuduRowInputFormat(KuduReaderConfig readerConfig, RowResultConvertor<Row> rowResultConvertor, KuduTableInfo tableInfo) {
        super(readerConfig, rowResultConvertor, tableInfo);
    }

    public KuduRowInputFormat(KuduReaderConfig readerConfig, RowResultConvertor<Row> rowResultConvertor, KuduTableInfo tableInfo, List<String> tableProjections) {
        super(readerConfig, rowResultConvertor, tableInfo, tableProjections);
    }

    public KuduRowInputFormat(KuduReaderConfig readerConfig, RowResultConvertor<Row> rowResultConvertor, KuduTableInfo tableInfo, List<KuduFilterInfo> tableFilters, List<String> tableProjections) {
        super(readerConfig, rowResultConvertor, tableInfo, tableFilters, tableProjections);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return TypeInformation.of(Row.class);
    }
}
