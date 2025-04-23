/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.oceanbase.omt;

import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.Record;
import com.oceanbase.connector.flink.table.TableId;
import com.oceanbase.connector.flink.table.TableInfo;
import com.oceanbase.omt.catalog.OceanBaseTable;
import com.oceanbase.omt.catalog.TableIdentifier;
import com.oceanbase.omt.sink.OceanBaseRowDataSerializationSchema;
import com.oceanbase.omt.source.starrocks.StarRocksType;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.CollectionUtil;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class DataChangeMapFunction extends RichMapFunction<RowData, DataChangeRecord> {

    private final OceanBaseTable oceanBaseTable;
    private final Map<TableIdentifier, TableIdentifier> tableIdRouteMapping;

    public DataChangeMapFunction(
            OceanBaseTable oceanBaseTable,
            Map<TableIdentifier, TableIdentifier> tableIdRouteMapping) {
        this.oceanBaseTable = oceanBaseTable;
        this.tableIdRouteMapping = tableIdRouteMapping;
    }

    @Override
    public DataChangeRecord map(RowData value) throws Exception {
        TableInfo tableInfo = convertOceanBaseTable(oceanBaseTable);
        OceanBaseRowDataSerializationSchema serializationSchema =
                new OceanBaseRowDataSerializationSchema(tableInfo);
        Record record = serializationSchema.serialize(value);
        return (DataChangeRecord) record;
    }

    public TableInfo convertOceanBaseTable(OceanBaseTable table) {
        // Apply Route rules
        if (!CollectionUtil.isNullOrEmpty(tableIdRouteMapping)) {
            TableIdentifier identifier = tableIdRouteMapping.get(table.getTableIdentifier());
            if (Objects.nonNull(identifier)) {
                table = table.buildTableWithTableId(identifier);
            }
        }
        TableId oceanBaseTableId =
                new TableId(
                        // Note: Do not rewrite it as a lambda expression to ensure that Kryo can
                        // serialize it correctly.
                        new TableId.Identifier() {
                            @Override
                            public String identifier(String schemaName, String tableName) {
                                return String.format("`%s`.`%s`", schemaName, tableName);
                            }
                        },
                        table.getDatabase(),
                        table.getTable());
        List<LogicalType> logicalTypes =
                table.getFields().stream()
                        .map(StarRocksType::toFlinkDataType)
                        .collect(Collectors.toList());
        TableInfo tableInfo =
                new TableInfo(
                        oceanBaseTableId,
                        table.getKeys(),
                        table.getFieldNames(),
                        logicalTypes,
                        null);
        return tableInfo;
    }
}
