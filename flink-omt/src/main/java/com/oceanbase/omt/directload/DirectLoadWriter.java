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

package com.oceanbase.omt.directload;

import com.oceanbase.connector.flink.OBDirectLoadConnectorOptions;
import com.oceanbase.connector.flink.directload.DirectLoader;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.RecordSerializationSchema;
import com.oceanbase.connector.flink.table.TableInfo;
import com.oceanbase.omt.catalog.TableIdentifier;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.CollectionUtil;

import org.apache.flink.shaded.curator5.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.oceanbase.connector.flink.directload.DirectLoader.createObObj;

/** The direct-load sink writer. see {@link SinkWriter}. */
public class DirectLoadWriter<T> implements SinkWriter<T> {
    private static final Logger LOG = LoggerFactory.getLogger(DirectLoadWriter.class);
    private static final int CORE_NUM = Runtime.getRuntime().availableProcessors();

    private final Map<TableIdentifier, DirectLoader> directLoadCache = new ConcurrentHashMap<>();
    private final RecordSerializationSchema<T> recordSerializer;
    private final OBDirectLoadConnectorOptions connectorOptions;
    private final List<TableIdentifier> tableIds;
    private final Map<Thread, List<DataChangeRecord>> bufferMap = Maps.newConcurrentMap();
    private final ExecutorService executorService;

    public DirectLoadWriter(
            OBDirectLoadConnectorOptions connectorOptions,
            RecordSerializationSchema<T> recordSerializer,
            List<TableIdentifier> tableIdList,
            int numberOfTaskSlots) {
        this.connectorOptions = connectorOptions;
        this.recordSerializer = recordSerializer;
        this.executorService =
                new ThreadPoolExecutor(
                        Math.min(numberOfTaskSlots, CORE_NUM),
                        Math.min(numberOfTaskSlots, CORE_NUM) * 2,
                        0,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1024),
                        Executors.defaultThreadFactory(),
                        new ThreadPoolExecutor.CallerRunsPolicy());
        this.tableIds = tableIdList;
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        DataChangeRecord record = (DataChangeRecord) recordSerializer.serialize(element);
        if (record == null) {
            return;
        }

        executorService.execute(
                () -> {
                    List<DataChangeRecord> buffer =
                            bufferMap.computeIfAbsent(
                                    Thread.currentThread(),
                                    recordBuffer ->
                                            Lists.newArrayListWithExpectedSize(
                                                    connectorOptions.getBufferSize()));
                    buffer.add(record);
                    if (buffer.size() >= connectorOptions.getBufferSize()) {
                        try {
                            flush(buffer);
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to flash to OceanBase.", e);
                        }
                    }
                });
    }

    private void flush(List<DataChangeRecord> buffer) throws ObDirectLoadException {
        if (CollectionUtils.isEmpty(buffer)) {
            return;
        }
        Map<TableIdentifier, ObDirectLoadBucket> bucketMap = new HashMap<>();
        for (DataChangeRecord dataChangeRecord : buffer) {
            TableInfo table = (TableInfo) dataChangeRecord.getTable();
            TableIdentifier tableId =
                    new TableIdentifier(
                            table.getTableId().getSchemaName(), table.getTableId().getTableName());
            bucketMap.putIfAbsent(tableId, new ObDirectLoadBucket());
            List<String> fieldNames = table.getFieldNames();
            ObObj[] array = new ObObj[fieldNames.size()];
            int index = 0;
            for (String fieldName : fieldNames) {
                array[index++] = createObObj(dataChangeRecord.getFieldValue(fieldName));
            }
            ObDirectLoadBucket obDirectLoadBucket = bucketMap.get(tableId);
            obDirectLoadBucket.addRow(array);
        }
        bucketMap.forEach(
                (tableId, obDirectLoadBucket) -> {
                    try {
                        DirectLoader directLoader =
                                directLoadCache.computeIfAbsent(
                                        tableId,
                                        tableIdentifier -> {
                                            DirectLoader loader =
                                                    DirectLoadUtils.buildDirectLoaderFromConnOption(
                                                            connectorOptions, tableId);
                                            try {
                                                loader.begin();
                                                LOG.info(
                                                        "The direct-load writer begin successfully with table {}.{}.",
                                                        tableId.getSchemaName(),
                                                        tableId.getTableName());
                                            } catch (SQLException e) {
                                                throw new RuntimeException(
                                                        "The direct-load writer begin failed. Please ensure that the tenant has sufficient computing resources for direct-load.",
                                                        e);
                                            }
                                            return loader;
                                        });
                        directLoader.write(obDirectLoadBucket);
                    } catch (SQLException e) {
                        throw new RuntimeException("Failed to flash to OceanBase.", e);
                    }
                });
        buffer.clear();
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (endOfInput) {
            executorService.shutdown();
            boolean termination = executorService.awaitTermination(600, TimeUnit.SECONDS);
            try {
                if (termination) {
                    for (List<DataChangeRecord> buffer : bufferMap.values()) {
                        flush(buffer);
                    }
                    if (!connectorOptions.getEnableMultiNodeWrite()) {
                        for (DirectLoader directLoader : directLoadCache.values()) {
                            directLoader.commit();
                        }
                    }
                    LOG.info("The direct-load write to oceanbase succeed.");
                } else {
                    throw new RuntimeException(
                            "The direct-load write to oceanbase timeout after 600 seconds.");
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to write to OceanBase.", e);
            }
        } else {
            // Flush but do not commit during flink checkpoint.
            //            try {
            //                for (List<DataChangeRecord> buffer : bufferMap.values()) {
            //                    flush(buffer);
            //                }
            //            } catch (Exception e) {
            //                throw new RuntimeException("Failed to write to OceanBase.", e);
            //            }
        }
    }

    @Override
    public void close() throws Exception {
        if (CollectionUtil.isNullOrEmpty(directLoadCache)) {
            directLoadCache.forEach((tableId, directLoader) -> directLoader.close());
        }
    }
}
