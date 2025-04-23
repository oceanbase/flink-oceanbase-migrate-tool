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

import com.oceanbase.connector.flink.OBDirectLoadConnectorOptions;
import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import com.oceanbase.connector.flink.connection.OceanBaseConnectionProvider;
import com.oceanbase.connector.flink.sink.OceanBaseRecordFlusher;
import com.oceanbase.connector.flink.sink.OceanBaseSink;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.DataChangeRecordData;
import com.oceanbase.omt.catalog.OceanBaseTable;
import com.oceanbase.omt.catalog.TableIdentifier;
import com.oceanbase.omt.directload.DirectLoadSink;
import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.OBMigrateConfig;
import com.oceanbase.omt.parser.Route;
import com.oceanbase.omt.utils.OBSinkType;
import com.oceanbase.omt.utils.OceanBaseUserInfo;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CollectionUtil;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class DatabaseSyncBase {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseSyncBase.class);
    protected MigrationConfig migrationConfig;

    public DatabaseSyncBase(MigrationConfig migrationConfig) {
        this.migrationConfig = migrationConfig;
    }

    public abstract Connection getConnection() throws SQLException;

    public abstract void checkRequiredOptions();

    public abstract List<OceanBaseTable> getObTables() throws Exception;

    public abstract void createTableInOb() throws SQLException;

    public void buildPipeline(StreamExecutionEnvironment env) throws Exception {
        List<OceanBaseTable> oceanBaseTables = getObTables();
        Map<TableIdentifier, TableIdentifier> tableIdRouteMapping = null;
        if (Objects.nonNull(applyRoutesRules(oceanBaseTables))) {
            tableIdRouteMapping = applyRoutesRules(oceanBaseTables).f1;
        }
        int sourceParallel =
                Integer.parseInt(
                        migrationConfig.getSource().getOther().getOrDefault("parallelism", "2"));

        SingleOutputStreamOperator<DataChangeRecord> source = null;
        DataStream<DataChangeRecord> recordDataStream = null;
        for (int i = 0; i < oceanBaseTables.size(); i++) {
            OceanBaseTable oceanBaseTable = oceanBaseTables.get(i);
            if (i == 0) {
                source =
                        env.addSource(buildSourceFunction(oceanBaseTable))
                                .setParallelism(sourceParallel)
                                .map(new DataChangeMapFunction(oceanBaseTable, tableIdRouteMapping))
                                .setParallelism(sourceParallel)
                                .name(buildSourceDescription(oceanBaseTable));
                continue;
            }
            SingleOutputStreamOperator<DataChangeRecord> otherSource =
                    env.addSource(buildSourceFunction(oceanBaseTable))
                            .setParallelism(sourceParallel)
                            .map(new DataChangeMapFunction(oceanBaseTable, tableIdRouteMapping))
                            .setParallelism(sourceParallel)
                            .name(buildSourceDescription(oceanBaseTable));
            if (Objects.isNull(recordDataStream)) {
                recordDataStream = source.union(otherSource);
                continue;
            }
            recordDataStream = recordDataStream.union(otherSource);
        }

        // For only one table need to be sync
        if (oceanBaseTables.size() == 1 && Objects.isNull(recordDataStream)) {
            recordDataStream = source;
        }
        assert recordDataStream != null;
        // Do sink
        OBSinkType obSinkType = OBSinkType.getValue(migrationConfig.getOceanbase().getType());
        if (obSinkType == OBSinkType.JDBC) {
            Sink<DataChangeRecord> rowDataOceanBaseSink = buildOceanBaseSink();
            recordDataStream.rebalance().sinkTo(rowDataOceanBaseSink);
        } else if (obSinkType == OBSinkType.DirectLoad) {
            Integer numberOfTaskSlots =
                    env.getConfiguration().get(TaskManagerOptions.NUM_TASK_SLOTS);
            int parallelism = env.getParallelism();
            Sink<DataChangeRecord> rowDataOceanBaseSink =
                    buildDirectLoadOceanBaseSink(numberOfTaskSlots);
            KeyedStream<DataChangeRecord, String> keyedStream =
                    recordDataStream.keyBy(key -> key.getTableId().identifier());
            keyedStream
                    .sinkTo(rowDataOceanBaseSink)
                    .setParallelism(Math.min(parallelism, oceanBaseTables.size()));
            // .setMaxParallelism(oceanBaseTables.size()); // Flink-1.17 not support!
        } else {
            throw new RuntimeException("Unknown OB sink type " + obSinkType);
        }
    }

    public abstract SourceFunction<RowData> buildSourceFunction(OceanBaseTable oceanBaseTable);

    protected OceanBaseSink<DataChangeRecord> buildOceanBaseSink() {
        OceanBaseConnectorOptions connectorOptions = buildConnectionOptions();
        OceanBaseConnectionProvider connectionProvider =
                new OceanBaseConnectionProvider(connectorOptions);
        OceanBaseRecordFlusher recordFlusher =
                new OceanBaseRecordFlusher(connectorOptions, connectionProvider);
        OceanBaseSink<DataChangeRecord> rowDataOceanBaseSink =
                new OceanBaseSink<>(
                        connectorOptions,
                        null,
                        new DoNothingSerializationSchema(),
                        simple(),
                        recordFlusher);
        return rowDataOceanBaseSink;
    }

    public OceanBaseConnectorOptions buildConnectionOptions() {
        OBMigrateConfig oceanbase = this.migrationConfig.getOceanbase();
        Map<String, String> other = oceanbase.getOther();
        ImmutableMap<String, String> configMap =
                ImmutableMap.<String, String>builder()
                        .put(OceanBaseConnectorOptions.URL.key(), oceanbase.getUrl())
                        .put(OceanBaseConnectorOptions.SCHEMA_NAME.key(), oceanbase.getSchemaName())
                        .put(OceanBaseConnectorOptions.USERNAME.key(), oceanbase.getUsername())
                        .put(OceanBaseConnectorOptions.PASSWORD.key(), oceanbase.getPassword())
                        .build();
        HashMap<String, String> config = new HashMap<>(other);
        config.putAll(configMap);
        return new OceanBaseConnectorOptions(config);
    }

    static DataChangeRecord.KeyExtractor simple() {
        return record -> {
            if (Objects.isNull(record)) {
                return null;
            } else {
                List<String> key = record.getTable().getKey();
                if (key.isEmpty()) {
                    return null;
                } else {
                    return new DataChangeRecordData(
                            key.stream().map(record::getFieldValue).toArray());
                }
            }
        };
    }

    protected String buildSourceDescription(OceanBaseTable oceanBaseTable) {
        String type = migrationConfig.getSource().getType();
        return String.format(
                "%s-%s.%s", type, oceanBaseTable.getDatabase(), oceanBaseTable.getTable());
    }

    // DirectLoad related.
    protected DirectLoadSink<DataChangeRecord> buildDirectLoadOceanBaseSink(int numberOfTaskSlots)
            throws Exception {
        OBDirectLoadConnectorOptions connectorOptions = buildDirectLoadConnectionOptions();
        List<TableIdentifier> tableIds =
                getObTables().stream()
                        .map(
                                oceanBaseTable ->
                                        new TableIdentifier(
                                                oceanBaseTable.getDatabase(),
                                                oceanBaseTable.getTable()))
                        .collect(Collectors.toList());
        DirectLoadSink directLoadSink =
                new DirectLoadSink(
                        connectorOptions,
                        new DoNothingSerializationSchema(),
                        tableIds,
                        numberOfTaskSlots);
        return directLoadSink;
    }

    public OBDirectLoadConnectorOptions buildDirectLoadConnectionOptions() {
        OBMigrateConfig oceanbase = this.migrationConfig.getOceanbase();
        Map<String, String> other = oceanbase.getOther();
        OceanBaseUserInfo userInfo =
                OceanBaseUserInfo.parse(
                        oceanbase
                                .getOther()
                                .getOrDefault(
                                        OBMigrateConfig.OB_DIRECT_LOAD_USERNAME,
                                        oceanbase.getUsername()));
        ImmutableMap<String, String> configMap =
                ImmutableMap.<String, String>builder()
                        .put(OBDirectLoadConnectorOptions.USERNAME.key(), userInfo.getUser())
                        .put(OBDirectLoadConnectorOptions.TENANT_NAME.key(), userInfo.getTenant())
                        .put(OBDirectLoadConnectorOptions.PASSWORD.key(), oceanbase.getPassword())
                        .build();
        HashMap<String, String> config = new HashMap<>(other);
        config.putAll(configMap);
        return new OBDirectLoadConnectorOptions(config);
    }

    // ============== Route section ===============

    /**
     * @return Tuple2<List<OceanBaseTable>, Map<TableIdentifier, TableIdentifier>>
     *     <p>List<OceanBaseTable>: For stp1, used in table-schema sync
     *     <p>Map<TableIdentifier, TableIdentifier>: For stp2, used in flink job
     */
    public Tuple2<List<OceanBaseTable>, Map<TableIdentifier, TableIdentifier>> applyRoutesRules(
            List<OceanBaseTable> oceanBaseTables) {
        List<Route> routes = migrationConfig.getRoutes();
        if (CollectionUtil.isNullOrEmpty(routes)) {
            return null;
        }

        // 1. Check not allow a table across multi rules
        List<String> sourceRules =
                routes.stream().map(Route::getSourceTable).collect(Collectors.toList());
        oceanBaseTables.forEach(
                oceanBaseTable -> {
                    String tableIdentifier = oceanBaseTable.getTableIdentifier().identifier();
                    int matchCount =
                            sourceRules.stream()
                                    .mapToInt(
                                            rule -> Pattern.matches(rule, tableIdentifier) ? 1 : 0)
                                    .sum();
                    if (matchCount > 1) {
                        throw new RuntimeException("Not allow a table match multi rules");
                    }
                });

        // TODO: Check table of source rule should has same schema

        // 2. Apply routs
        HashMap<TableIdentifier, TableIdentifier> tableIdRouteMapping = new HashMap<>();
        List<OceanBaseTable> routedTables =
                oceanBaseTables.stream()
                        .map(
                                oceanBaseTable -> {
                                    String tableIdentifier =
                                            oceanBaseTable.getTableIdentifier().identifier();
                                    OceanBaseTable table = null;
                                    for (Route route : routes) {
                                        if (Pattern.matches(
                                                route.getSourceTable(), tableIdentifier)) {
                                            String[] split = route.getSinkTable().split("\\.");
                                            TableIdentifier identifier =
                                                    new TableIdentifier(split[0], split[1]);
                                            table =
                                                    oceanBaseTable.buildTableWithTableId(
                                                            identifier);
                                        }
                                    }
                                    if (Objects.nonNull(table)) {
                                        tableIdRouteMapping.put(
                                                oceanBaseTable.getTableIdentifier(),
                                                new TableIdentifier(
                                                        table.getDatabase(), table.getTable()));
                                        return table;
                                    } else {
                                        return oceanBaseTable;
                                    }
                                })
                        .distinct()
                        .collect(Collectors.toList());

        if (CollectionUtil.isNullOrEmpty(tableIdRouteMapping)) {
            return null;
        }

        return Tuple2.of(routedTables, tableIdRouteMapping);
    }
}
