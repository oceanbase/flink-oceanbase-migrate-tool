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
package com.oceanbase.omt.source.clickhouse;

import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.catalog.OceanBaseTable;
import com.oceanbase.omt.partition.ClickHousePartitionUtils;
import com.oceanbase.omt.partition.PartitionInfo;
import com.oceanbase.omt.source.starrocks.StarRocksDDLGenTools;

import org.apache.flink.util.CollectionUtil;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class ClickHouseDDLGenTools {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDDLGenTools.class);

    public static List<String> buildOBCreateTableDDL(
            List<OceanBaseTable> schemaList, String columnStoreType) {
        String prefix = "CREATE TABLE IF NOT EXISTS %s (%s %s)";
        String columnStoreSql = "";
        if (StringUtils.isNotBlank(columnStoreType)) {
            if (columnStoreType.equalsIgnoreCase("CST")) {
                columnStoreSql = " WITH COLUMN GROUP(each column)";
            }
            if (columnStoreType.equalsIgnoreCase("RCMT")) {
                columnStoreSql = " WITH COLUMN GROUP(all columns, each column)";
            }
        }
        // name type default-value comment
        String colFormat = "`%s` %s %s %s";
        String finalColumnStoreSql = columnStoreSql;
        List<String> tableDDLs =
                schemaList.stream()
                        .map(
                                schema -> {
                                    // Relate column
                                    List<OceanBaseColumn> fields = schema.getFields();
                                    String filedStr =
                                            fields.stream()
                                                    .map(
                                                            fieldSchema -> {
                                                                String oceanBaseType =
                                                                        ClickHouseType
                                                                                .toOceanBaseMySQLType(
                                                                                        fieldSchema);
                                                                return String.format(
                                                                        colFormat,
                                                                        fieldSchema.getName(),
                                                                        oceanBaseType,
                                                                        buildDefaultValue(
                                                                                fieldSchema),
                                                                        buildComment(fieldSchema));
                                                            })
                                                    .collect(Collectors.joining(","));

                                    // Relate table
                                    String tableName =
                                            String.format(
                                                    "`%s`.`%s`",
                                                    schema.getDatabase(), schema.getTable());
                                    List<String> keys = schema.getKeys();
                                    String primaryKeyStr = "";
                                    if (!CollectionUtil.isNullOrEmpty(keys)) {

                                        List<String> collectKeys =
                                                keys.stream()
                                                        .distinct()
                                                        .collect(Collectors.toList());

                                        primaryKeyStr =
                                                String.format(
                                                        ",PRIMARY KEY(%s)",
                                                        String.join(",", collectKeys));
                                    }

                                    String noPartitionDDl =
                                            String.format(
                                                    prefix, tableName, filedStr, primaryKeyStr);
                                    String obPartitionWithDDL =
                                            buildOBPartitionWithDDL(
                                                    noPartitionDDl, schema.getPartition());
                                    return obPartitionWithDDL + finalColumnStoreSql;
                                })
                        .collect(Collectors.toList());
        return tableDDLs;
    }

    private static String buildDefaultValue(OceanBaseColumn fieldSchema) {
        String defaultValue = "";
        if (!Strings.isNullOrEmpty(fieldSchema.getDefaultValue())) {
            defaultValue = String.format("DEFAULT %s", fieldSchema.getDefaultValue());
        }
        return defaultValue;
    }

    private static String buildComment(OceanBaseColumn fieldSchema) {
        String comment = "";
        if (!Strings.isNullOrEmpty(fieldSchema.getComment())) {
            comment = String.format("COMMENT '%s'", fieldSchema.getComment());
        }
        return comment;
    }

    public static String buildOBPartitionWithDDL(String ddl, List<PartitionInfo> partitions) {
        return ClickHousePartitionUtils.buildOBPartitionWithDDL(ddl, partitions);
    }
}
