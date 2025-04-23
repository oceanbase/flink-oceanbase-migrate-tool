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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MultiSinkProcessFunction extends ProcessFunction<DataChangeRecord, DataChangeRecord> {
    private transient Map<String, OutputTag<DataChangeRecord>> recordOutputTags =
            new ConcurrentHashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void processElement(
            DataChangeRecord dataChangeRecord,
            ProcessFunction<DataChangeRecord, DataChangeRecord>.Context context,
            Collector<DataChangeRecord> collector)
            throws Exception {
        String identifier = dataChangeRecord.getTableId().identifier();
        OutputTag<DataChangeRecord> outputTag =
                recordOutputTags.computeIfAbsent(
                        identifier, MultiSinkProcessFunction::createRecordOutputTag);
        context.output(outputTag, dataChangeRecord);
    }

    public static OutputTag<DataChangeRecord> createRecordOutputTag(String identifier) {
        return new OutputTag<DataChangeRecord>(identifier) {};
    }
}
