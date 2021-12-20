/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.colloh.flink.kudu.connector.table.catalog;

import org.apache.commons.compress.utils.Sets;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.apache.flink.table.catalog.CommonCatalogOptions.CATALOG_TYPE;
import static org.colloh.flink.kudu.connector.table.KuduDynamicTableSourceSinkFactory.IDENTIFIER;
import static org.colloh.flink.kudu.connector.table.KuduDynamicTableSourceSinkFactory.KUDU_MASTERS;
import static org.colloh.flink.kudu.connector.table.KuduDynamicTableSourceSinkFactory.KUDU_TABLE;

/**
 * Factory for {@link KuduCatalog}.
 */
@Internal
public class KuduCatalogFactory implements CatalogFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KuduCatalogFactory.class);

    @Override
    public Catalog createCatalog(Context context) {
        String kuduMasters = context.getConfiguration().get(KUDU_MASTERS);
        String catalogName = context.getName();
        return new KuduCatalog(catalogName, kuduMasters);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Sets.newHashSet(CATALOG_TYPE, KUDU_MASTERS);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Sets.newHashSet(KUDU_TABLE);
    }
    
        @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CATALOG_TYPE, KUDU);
        context.put(CATALOG_PROPERTY_VERSION, "1"); // backwards compatibility
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        properties.add(KUDU_MASTERS.key());

        return properties;
    }
}
