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

package com.oceanbase.omt.source.starrocks;

import com.oceanbase.omt.DatabaseSyncBase;
import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.catalog.OceanBaseTable;
import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.OBMigrateConfig;
import com.oceanbase.omt.parser.SourceMigrateConfig;
import com.oceanbase.omt.partition.PartitionInfo;
import com.oceanbase.omt.utils.DataSourceUtils;
import com.oceanbase.omt.utils.OceanBaseJdbcUtils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import com.starrocks.connector.flink.StarRocksSource;
import com.starrocks.connector.flink.catalog.StarRocksCatalogException;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.oceanbase.omt.source.starrocks.StarRocksJdbcUtils.executeDoubleColumnStatement;
import static com.oceanbase.omt.source.starrocks.StarRocksJdbcUtils.obtainPartitionInfo;

public class StarRocksDatabaseSync extends DatabaseSyncBase {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDatabaseSync.class);
    private List<OceanBaseTable> oceanBaseTables;

    public StarRocksDatabaseSync(MigrationConfig migrationConfig) {
        super(migrationConfig);
    }

    @Override
    public Connection getConnection() throws SQLException {
        SourceMigrateConfig config = migrationConfig.getSource();
        try {
            return DataSourceUtils.getSourceDataSource(config).getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void checkRequiredOptions() {
        // TODO: Add check for require options
    }

    @Override
    public List<OceanBaseTable> getObTables() {
        // Only as a simple cache.
        if (Objects.nonNull(oceanBaseTables)) {
            return oceanBaseTables;
        }

        SourceMigrateConfig source = migrationConfig.getSource();
        List<Tuple2<String, String>> tableNames = getTableNames(source.getTables());
        List<OceanBaseTable> oceanBaseTableList =
                tableNames.stream()
                        .map(ruleTable -> getTable(ruleTable.f0, ruleTable.f1))
                        .collect(Collectors.toList());
        this.oceanBaseTables = oceanBaseTableList;
        return oceanBaseTableList;
    }

    @Override
    public void createTableInOb() throws SQLException {
        List<OceanBaseTable> obTables = getObTables();
        OBMigrateConfig obMigrateConfig = this.migrationConfig.getOceanbase();
        String columnStoreType = obMigrateConfig.getColumnStoreType();
        if (Objects.nonNull(applyRoutesRules(obTables))) {
            obTables = applyRoutesRules(obTables).f0;
        }
        // Create Database first
        List<String> databases =
                obTables.stream()
                        .map(OceanBaseTable::getDatabase)
                        .distinct()
                        .collect(Collectors.toList());
        for (String database : databases) {
            if (!OceanBaseJdbcUtils.databaseExists(migrationConfig, database)) {
                String databaseDDL = String.format("CREATE DATABASE IF NOT EXISTS %s", database);
                OceanBaseJdbcUtils.executeUpdateStatement(migrationConfig, databaseDDL);
            }
        }
        List<String> obCreateTableDDLs =
                StarRocksDDLGenTools.buildOBCreateTableDDL(obTables, columnStoreType);

        for (String createTableDDL : obCreateTableDDLs) {
            LOG.info("Create table with DDL: {}", createTableDDL);
            OceanBaseJdbcUtils.executeUpdateStatement(migrationConfig, createTableDDL);
        }
    }

    public OceanBaseTable getTable(String databaseName, String tableName)
            throws StarRocksCatalogException {
        final String tableSchemaQuery =
                "SELECT `COLUMN_NAME`, `DATA_TYPE`, `ORDINAL_POSITION`, `COLUMN_SIZE`, `DECIMAL_DIGITS`, "
                        + "`IS_NULLABLE`, `COLUMN_KEY`, `COLUMN_COMMENT`,`COLUMN_DEFAULT`,`COLUMN_TYPE` FROM `information_schema`.`COLUMNS` "
                        + "WHERE `TABLE_SCHEMA`=? AND `TABLE_NAME`=?;";

        List<OceanBaseColumn> columns = new ArrayList<>();
        List<String> tableKeys = new ArrayList<>();
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(tableSchemaQuery)) {
                statement.setObject(1, databaseName);
                statement.setObject(2, tableName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String name = resultSet.getString("COLUMN_NAME");
                        String type = resultSet.getString("DATA_TYPE");
                        Integer size = resultSet.getInt("COLUMN_SIZE");
                        if (resultSet.wasNull()) {
                            size = null;
                        }
                        // mysql does not have boolean type, and starrocks
                        // `information_schema`.`COLUMNS` will return
                        // a "tinyint" data type for both StarRocks BOOLEAN and TINYINT type,
                        // Distinguish them by
                        // column size, and the size of BOOLEAN is null
                        if ("tinyint".equalsIgnoreCase(type) && size == null) {
                            type = "boolean";
                        }
                        int position = resultSet.getInt("ORDINAL_POSITION");
                        Integer scale = resultSet.getInt("DECIMAL_DIGITS");
                        if (resultSet.wasNull()) {
                            scale = null;
                        }

                        String isNullable = resultSet.getString("IS_NULLABLE");
                        String comment = resultSet.getString("COLUMN_COMMENT");
                        String defaultValue = resultSet.getString("COLUMN_DEFAULT");
                        String columnType = resultSet.getString("COLUMN_TYPE");
                        OceanBaseColumn column =
                                OceanBaseColumn.FieldSchemaBuilder.aFieldSchema()
                                        .withName(name)
                                        .withOrdinalPosition(position - 1)
                                        .withTypeString(type)
                                        .withColumnSize(size)
                                        .withColumnType(columnType)
                                        .withDataType(type)
                                        .withNumericScale(scale)
                                        .withNullable(
                                                isNullable == null
                                                        || !isNullable.equalsIgnoreCase("NO"))
                                        .withComment(comment)
                                        .withDefaultValue(defaultValue)
                                        .build();
                        columns.add(column);
                        // Only primary key table has value in this field. and the value is "PRI"
                        String columnKey = resultSet.getString("COLUMN_KEY");
                        getKeys(columnKey, tableKeys, column);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to get table %s.%s", databaseName, tableName), e);
        }
        List<PartitionInfo> partitions = getPartitions(databaseName, tableName, columns);
        OceanBaseTable tableSchema = null;
        if (!columns.isEmpty()) {
            tableSchema =
                    OceanBaseTable.TableSchemaBuilder.aTableSchema()
                            .withDatabase(databaseName)
                            .withTable(tableName)
                            .withFields(columns)
                            .withKeys(tableKeys)
                            .withPartition(partitions)
                            .build();
        }
        return tableSchema;
    }

    private static void getKeys(String columnKey, List<String> tableKeys, OceanBaseColumn column) {
        if (!StringUtils.isNullOrWhitespaceOnly(columnKey) && columnKey.equals("PRI")
                || columnKey.equals("UNI")
                || columnKey.equals("MUL")) {
            tableKeys.add(column.getName());
        }
    }

    public List<PartitionInfo> getPartitions(
            String databaseName, String tableName, List<OceanBaseColumn> columns) {
        try {
            return obtainPartitionInfo(getConnection(), databaseName, tableName, columns);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get table names", e);
        }
    }

    public List<Tuple2<String, String>> getTableNames(String tables) {
        String sql =
                "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.`TABLES` WHERE CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) REGEXP ?";
        try {
            return executeDoubleColumnStatement(getConnection(), sql, tables);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get table names", e);
        }
    }

    public SourceFunction<RowData> buildSourceFunction(OceanBaseTable oceanBaseTable) {
        SourceMigrateConfig dbMigrateConfig = migrationConfig.getSource();
        Map<String, String> other = dbMigrateConfig.getOther();

        // Merge yaml all the config to StarRocksSourceOptions
        ImmutableMap<String, String> configMap =
                ImmutableMap.<String, String>builder()
                        .put(
                                StarRocksSourceOptions.JDBC_URL.key(),
                                other.get(StarRocksConfig.JDBC_URL))
                        .put(
                                StarRocksSourceOptions.USERNAME.key(),
                                other.get(StarRocksConfig.USERNAME))
                        .put(
                                StarRocksSourceOptions.PASSWORD.key(),
                                other.get(StarRocksConfig.PASSWORD))
                        .put(StarRocksSourceOptions.TABLE_NAME.key(), oceanBaseTable.getTable())
                        .put(
                                StarRocksSourceOptions.DATABASE_NAME.key(),
                                oceanBaseTable.getDatabase())
                        .build();
        HashMap<String, String> config = new HashMap<>(dbMigrateConfig.getOther());
        config.putAll(configMap);
        Configuration configuration = Configuration.fromMap(config);
        StarRocksSourceOptions options =
                new StarRocksSourceOptions(configuration, configuration.toMap());

        TableSchema.Builder builder = TableSchema.builder();
        oceanBaseTable
                .getFields()
                .forEach(
                        oceanBaseColumn -> {
                            builder.field(
                                    oceanBaseColumn.getName(),
                                    DataTypes.of(StarRocksType.toFlinkDataType(oceanBaseColumn)));
                        });

        TableSchema tableSchema = builder.build();
        return StarRocksSource.source(tableSchema, options);
    }
}
