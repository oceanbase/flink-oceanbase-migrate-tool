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
package com.oceanbase.omt.source.doris;

import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.partition.PartitionInfo;

import org.apache.flink.api.java.tuple.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Doris JDBC utility class, provides utility methods for Doris database connection and query */
public class DorisJdbcUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DorisJdbcUtils.class);

    public static List<Tuple2<String, String>> executeDoubleColumnStatement(
            Connection connection, String sql, Object... params) {
        try (Connection conn = connection;
                PreparedStatement statement = conn.prepareStatement(sql)) {
            List<Tuple2<String, String>> columnValues = new ArrayList<>();
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String columnValue1 = rs.getString(1);
                    String columnValue2 = rs.getString(2);
                    columnValues.add(Tuple2.of(columnValue1, columnValue2));
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to execute sql: %s", sql), e);
        }
    }

    public static List<PartitionInfo> obtainPartitionInfo(
            Connection connection,
            String databaseName,
            String tableName,
            List<OceanBaseColumn> columns) {
        String sql = "SHOW PARTITIONS FROM %s.%s";
        try (Connection conn = connection;
                PreparedStatement statement =
                        conn.prepareStatement(String.format(sql, databaseName, tableName));
                ResultSet rs = statement.executeQuery()) {
            List<PartitionInfo> partitionInfos = new ArrayList<>();
            while (rs.next()) {
                String range = null;
                String list = null;
                String partitionId = rs.getString("PartitionId");
                String partitionName = rs.getString("PartitionName");
                String partitionKey = rs.getString("PartitionKey");
                String distributionKey = rs.getString("DistributionKey");
                Integer buckets = rs.getInt("Buckets");
                range = rs.getString("Range");
                if (Objects.nonNull(range) && !range.isEmpty() && !range.contains("..")) {
                    list = range;
                    range = null;
                }
                String[] split = partitionKey.split(",");
                List<String> partitionKeyTypes = new ArrayList<>();
                for (String everyKey : split) {
                    columns.stream()
                            .filter(e -> e.getName().equals(everyKey.trim()))
                            .findFirst()
                            .ifPresent(
                                    oceanBaseColumn ->
                                            partitionKeyTypes.add(oceanBaseColumn.getDataType()));
                }
                PartitionInfo partitionInfo =
                        new PartitionInfo()
                                .withPartitionId(partitionId)
                                .withPartitionName(partitionName)
                                .withPartitionKey(partitionKey)
                                .withDistributionKey(distributionKey)
                                .withRange(range)
                                .withBuckets(buckets)
                                .withPartitionKeyType(partitionKeyTypes)
                                .withList(list);
                partitionInfos.add(partitionInfo);
            }
            return partitionInfos;
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to execute sql: %s", sql), e);
        }
    }
}
