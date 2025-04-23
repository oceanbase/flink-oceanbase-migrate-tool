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

import com.oceanbase.connector.flink.OBDirectLoadConnectorOptions;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.omt.DataChangeMapFunction;
import com.oceanbase.omt.DoNothingSerializationSchema;
import com.oceanbase.omt.catalog.OceanBaseTable;
import com.oceanbase.omt.catalog.TableIdentifier;
import com.oceanbase.omt.directload.DirectLoadSink;
import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.utils.OBSinkType;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Deprecated
public class StarRocksDataBaseDirectLoadSync extends StarRocksDatabaseSync {
    public StarRocksDataBaseDirectLoadSync(MigrationConfig migrationConfig) {
        super(migrationConfig);
    }

    public void buildPipelineX(
            StreamExecutionEnvironment env, List<OceanBaseTable> oceanBaseTableLL)
            throws Exception {
        List<OceanBaseTable> oceanBaseTables = oceanBaseTableLL;
        Map<TableIdentifier, TableIdentifier> tableIdRouteMapping = null;
        if (Objects.nonNull(applyRoutesRules(oceanBaseTables))) {
            tableIdRouteMapping = applyRoutesRules(oceanBaseTables).f1;
        }

        SingleOutputStreamOperator<DataChangeRecord> source = null;
        DataStream<DataChangeRecord> recordDataStream = null;
        for (int i = 0; i < oceanBaseTables.size(); i++) {
            OceanBaseTable oceanBaseTable = oceanBaseTables.get(i);
            if (i == 0) {
                source =
                        env.addSource(buildSourceFunction(oceanBaseTable))
                                .map(new DataChangeMapFunction(oceanBaseTable, tableIdRouteMapping))
                                .name(buildSourceDescription(oceanBaseTable));
                recordDataStream = source;
                continue;
            }
            SingleOutputStreamOperator<DataChangeRecord> otherSource =
                    env.addSource(buildSourceFunction(oceanBaseTable))
                            .map(new DataChangeMapFunction(oceanBaseTable, tableIdRouteMapping))
                            .name(buildSourceDescription(oceanBaseTable));
            if (Objects.isNull(recordDataStream)) {
                recordDataStream = source.union(otherSource);
            }
            recordDataStream = recordDataStream.union(otherSource);
        }

        // Do sink
        OBSinkType obSinkType = OBSinkType.getValue(migrationConfig.getOceanbase().getType());
        if (obSinkType == OBSinkType.JDBC) {
            Sink<DataChangeRecord> rowDataOceanBaseSink = buildOceanBaseSink();
            recordDataStream.sinkTo(rowDataOceanBaseSink);
        } else if (obSinkType == OBSinkType.DirectLoad) {
            Integer numberOfTaskSlots =
                    env.getConfiguration().get(TaskManagerOptions.NUM_TASK_SLOTS);
            Sink<DataChangeRecord> rowDataOceanBaseSink =
                    buildDirectLoadOceanBaseSink(numberOfTaskSlots, oceanBaseTables);
            recordDataStream.sinkTo(rowDataOceanBaseSink);
        } else {
            throw new RuntimeException("Unknown OB sink type " + obSinkType);
        }
    }

    public void buildPipelineX(StreamExecutionEnvironment env) throws Exception {
        for (OceanBaseTable obTable : getObTables()) {
            buildPipelineX(env, Collections.singletonList(obTable));
            env.execute();
        }
    }

    protected DirectLoadSink<DataChangeRecord> buildDirectLoadOceanBaseSink(
            int numberOfTaskSlots, List<OceanBaseTable> oceanBaseTables) throws Exception {
        OBDirectLoadConnectorOptions connectorOptions = buildDirectLoadConnectionOptions();
        List<TableIdentifier> tableIds =
                oceanBaseTables.stream()
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
}
