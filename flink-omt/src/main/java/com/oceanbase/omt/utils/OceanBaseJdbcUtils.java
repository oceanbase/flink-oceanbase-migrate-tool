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

package com.oceanbase.omt.utils;

import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.OBMigrateConfig;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class OceanBaseJdbcUtils {
    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseJdbcUtils.class);

    public static Connection getConnection(MigrationConfig migrationConfig) throws SQLException {
        OBMigrateConfig oceanbase = migrationConfig.getOceanbase();
        try {
            return DataSourceUtils.getOBDataSource(oceanbase).getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> executeSingleColumnStatement(
            MigrationConfig migrationConfig, String sql, Object... params) {
        try (Connection conn = getConnection(migrationConfig);
                PreparedStatement statement = conn.prepareStatement(sql)) {
            List<String> columnValues = new ArrayList<>();
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String columnValue = rs.getString(1);
                    columnValues.add(columnValue);
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to execute sql: %s", sql), e);
        }
    }

    public static void executeUpdateStatement(MigrationConfig migrationConfig, String sql)
            throws SQLException {
        try (Connection connection = getConnection(migrationConfig);
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    public static boolean databaseExists(MigrationConfig migrationConfig, String databaseName) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        String querySql =
                String.format(
                        "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA` WHERE SCHEMA_NAME = '%s';",
                        databaseName);
        try {
            List<String> dbList = executeSingleColumnStatement(migrationConfig, querySql);
            return !dbList.isEmpty();
        } catch (Exception e) {
            LOG.error(
                    "Failed to check database exist, database: {}, sql: {}",
                    databaseName,
                    querySql,
                    e);
            throw new RuntimeException(
                    String.format("Failed to check database exist, database: %s", databaseName), e);
        }
    }
}
