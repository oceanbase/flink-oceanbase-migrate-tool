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

package com.oceanbase.omt.type;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class ArrayDataParserTest {

    @Test
    public void testParseArray() {
        // Constructing the example array
        GenericArrayData[] outerArray =
                new GenericArrayData[] {
                    new GenericArrayData(new String[] {"3", "1"}),
                    new GenericArrayData(new String[] {"5"})
                };

        GenericArrayData arrayData = new GenericArrayData(outerArray);
        GenericArrayData genericArrayData =
                new GenericArrayData(new GenericArrayData[] {arrayData});

        // Parsing the array data
        String parsedResult = parseArrayData(genericArrayData, 1);
        Assert.assertEquals("[[[3,1],[5]]]", parsedResult);

        GenericArrayData arrayData1 = new GenericArrayData(outerArray);
        GenericArrayData genericArrayData1 =
                new GenericArrayData(new GenericArrayData[] {arrayData, arrayData1});
        String parsedResult1 = parseArrayData(genericArrayData1, 1);
        Assert.assertEquals("[[[3,1],[5]],[[3,1],[5]]]", parsedResult1);
    }

    public static String parseArrayData(GenericArrayData arrayData, int depth) {

        if (depth >= 6) {
            throw new IllegalArgumentException(
                    "Array nesting depth exceeds maximum allowed level of 6.");
        }
        StringBuilder result = new StringBuilder();
        result.append("[");

        Object[] objectArray = arrayData.toObjectArray();
        for (int i = 0; i < objectArray.length; i++) {
            Object element = objectArray[i];
            if (i > 0) result.append(",");
            if (element instanceof GenericArrayData) {
                result.append(parseArrayData((GenericArrayData) element, depth + 1));
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

    @Test
    public void testParseBinaryArray() {
        // Constructing the example array
        BinaryArrayData binaryArrayData = BinaryArrayData.fromPrimitiveArray(new int[] {1, 2});

        String s = parseArrayData(binaryArrayData, new ArrayType(new BooleanType()), 1);
        System.out.println(s);
    }

    public static String parseArrayData(BinaryArrayData arrayData, LogicalType type, int depth) {
        if (depth >= 6) {
            throw new IllegalArgumentException(
                    "Array nesting depth exceeds maximum allowed level of 6.");
        }

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
                String parsed = parseArrayData((BinaryArrayData) data.get(i), lt, depth + 1);
                result.append(parsed);
            }
        } else {
            String dataStr =
                    data.stream()
                            .map(
                                    element -> {
                                        if (element instanceof Integer) {
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

    private List<Object> convertNestedArray(ArrayData arrData, LogicalType type) {
        if (arrData instanceof GenericArrayData) {
            return Lists.newArrayList(((GenericArrayData) arrData).toObjectArray());
        }
        if (arrData instanceof BinaryArrayData) {
            LogicalType lt = ((ArrayType) type).getElementType();
            List<Object> data = Lists.newArrayList(((BinaryArrayData) arrData).toObjectArray(lt));
            if (LogicalTypeRoot.ARRAY.equals(lt.getTypeRoot())) {
                // traversal of the nested array
                return data.parallelStream()
                        .map(arr -> convertNestedArray((ArrayData) arr, lt))
                        .collect(Collectors.toList());
            }
            return data;
        }
        throw new UnsupportedOperationException(
                String.format("Unsupported array data: %s", arrData.getClass()));
    }
}
