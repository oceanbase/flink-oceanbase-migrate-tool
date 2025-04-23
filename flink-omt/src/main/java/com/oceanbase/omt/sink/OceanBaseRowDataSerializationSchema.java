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

package com.oceanbase.omt.sink;

import com.oceanbase.connector.flink.table.AbstractRecordSerializationSchema;
import com.oceanbase.connector.flink.table.DataChangeRecord;
import com.oceanbase.connector.flink.table.Record;
import com.oceanbase.connector.flink.table.SerializationRuntimeConverter;
import com.oceanbase.connector.flink.table.TableInfo;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava31.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OceanBaseRowDataSerializationSchema
        extends AbstractRecordSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(OceanBaseRowDataSerializationSchema.class);

    private final TableInfo tableInfo;
    private final RowData.FieldGetter[] fieldGetters;
    private final SerializationRuntimeConverter[] fieldConverters;

    public OceanBaseRowDataSerializationSchema(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        this.fieldGetters =
                IntStream.range(0, tableInfo.getDataTypes().size())
                        .boxed()
                        .map(i -> RowData.createFieldGetter(tableInfo.getDataTypes().get(i), i))
                        .toArray(RowData.FieldGetter[]::new);
        this.fieldConverters =
                tableInfo.getDataTypes().stream()
                        .map(this::getOrCreateConverter)
                        .toArray(SerializationRuntimeConverter[]::new);
    }

    @Override
    public Record serialize(RowData rowData) {
        Object[] values = new Object[fieldGetters.length];
        for (int i = 0; i < fieldGetters.length; i++) {
            values[i] = fieldConverters[i].convert(fieldGetters[i].getFieldOrNull(rowData));
        }
        return new DataChangeRecord(
                tableInfo,
                (rowData.getRowKind() == RowKind.INSERT
                                || rowData.getRowKind() == RowKind.UPDATE_AFTER)
                        ? DataChangeRecord.Type.UPSERT
                        : DataChangeRecord.Type.DELETE,
                values);
    }

    @Override
    protected SerializationRuntimeConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
            case BIGINT:
            case INTERVAL_DAY_TIME:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case VARBINARY:
                return data -> data;
            case CHAR:
            case VARCHAR:
                return Object::toString;
            case DATE:
                return data -> Date.valueOf(LocalDate.ofEpochDay((int) data));
            case TIME_WITHOUT_TIME_ZONE:
                return data -> Time.valueOf(LocalTime.ofNanoOfDay((int) data * 1_000_000L));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return data -> ((TimestampData) data).toTimestamp();
            case DECIMAL:
                return data -> ((DecimalData) data).toBigDecimal();
            case ARRAY:
                return data -> {
                    int depth = checkArrayDataNestDepth(type);
                    if (depth > 6) {
                        LOG.warn("Array nesting depth exceeds maximum allowed level of 6.");
                    }
                    if (data instanceof BinaryArrayData) {
                        return parseArrayData((BinaryArrayData) data, type);
                    } else if (data instanceof GenericArrayData) {
                        return parseArrayData((GenericArrayData) data);
                    } else {
                        throw new UnsupportedOperationException(
                                "Unknown array data-type: " + data.getClass().getSimpleName());
                    }
                };
            case MAP:
                return data -> {
                    Map<Object, Object> map = convertNestedMap((MapData) data, type);
                    return map.toString();
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private int checkArrayDataNestDepth(LogicalType type) {
        LogicalType lt = ((ArrayType) type).getElementType();
        int depth = 1;
        while (LogicalTypeRoot.ARRAY.equals(lt.getTypeRoot())) {
            lt = ((ArrayType) lt).getElementType();
            depth++;
        }
        return depth;
    }

    private String parseArrayData(BinaryArrayData arrayData, LogicalType type) {
        StringBuilder result = new StringBuilder();

        result.append("[");
        LogicalType lt = ((ArrayType) type).getElementType();
        List<Object> data = Lists.newArrayList((arrayData).toObjectArray(lt));
        if (LogicalTypeRoot.ARRAY.equals(lt.getTypeRoot())) {
            // traversal of the nested array
            for (int i = 0; i < data.size(); i++) {
                if (i > 0) {
                    result.append(",");
                }
                String parsed = parseArrayData((BinaryArrayData) data.get(i), lt);
                result.append(parsed);
            }
        } else {
            String dataStr =
                    data.stream()
                            .map(
                                    element -> {
                                        if (element instanceof BinaryStringData) {
                                            return String.format("\"%s\"", element);
                                        } else if (element instanceof Integer) {
                                            return element.toString();
                                        } else if (element instanceof Boolean) {
                                            return element.toString();
                                        } else if (element instanceof Long) {
                                            return element.toString();
                                        } else if (element instanceof Short) {
                                            return element.toString();
                                        } else if (element instanceof Float) {
                                            return element.toString();
                                        } else if (element instanceof Double) {
                                            return element.toString();
                                        } else if (element instanceof Byte) {
                                            return element.toString();
                                        } else {
                                            // Handle other types as needed
                                            return element.toString();
                                        }
                                    })
                            .collect(Collectors.joining(","));
            result.append(dataStr);
        }
        result.append("]");
        return result.toString();
    }

    public static String parseArrayData(GenericArrayData arrayData) {

        StringBuilder result = new StringBuilder();
        result.append("[");

        Object[] objectArray = arrayData.toObjectArray();
        for (int i = 0; i < objectArray.length; i++) {
            Object element = objectArray[i];
            if (i > 0) result.append(",");
            if (element instanceof GenericArrayData) {
                result.append(parseArrayData((GenericArrayData) element));
            } else if (element instanceof BinaryStringData) {
                result.append(String.format("\"%s\"", element));
            } else if (element instanceof Integer) {
                result.append(element.toString());
            } else if (element instanceof Boolean) {
                result.append(element);
            } else if (element instanceof Long) {
                result.append(element);
            } else if (element instanceof Short) {
                result.append(element);
            } else if (element instanceof Float) {
                result.append(element);
            } else if (element instanceof Double) {
                result.append(element);
            } else if (element instanceof Byte) {
                result.append(element);
            } else {
                // Handle other types as needed
                result.append(element.toString());
            }
        }

        result.append("]");
        return result.toString();
    }

    private Map<Object, Object> convertNestedMap(MapData mapData, LogicalType type) {
        if (mapData instanceof GenericMapData) {
            HashMap<Object, Object> m = Maps.newHashMap();
            for (Object k :
                    ((GenericArrayData) ((GenericMapData) mapData).keyArray()).toObjectArray()) {
                m.put(k, ((GenericMapData) mapData).get(k));
            }
            return m;
        }
        if (mapData instanceof BinaryMapData) {
            Map<Object, Object> result = Maps.newHashMap();
            LogicalType valType = ((MapType) type).getValueType();
            Map<?, ?> javaMap =
                    ((BinaryMapData) mapData).toJavaMap(((MapType) type).getKeyType(), valType);
            for (Map.Entry<?, ?> en : javaMap.entrySet()) {
                if (LogicalTypeRoot.MAP.equals(valType.getTypeRoot())) {
                    // traversal of the nested map
                    result.put(
                            en.getKey().toString(),
                            convertNestedMap((MapData) en.getValue(), valType));
                    continue;
                }
                result.put(en.getKey().toString(), en.getValue());
            }
            return result;
        }
        throw new UnsupportedOperationException(
                String.format("Unsupported map data: %s", mapData.getClass()));
    }
}
