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

import com.oceanbase.omt.catalog.OceanBaseColumn;
import com.oceanbase.omt.catalog.OceanBaseMySQLType;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class StarRocksType {
    // Numeric
    public static final String BOOLEAN = "BOOLEAN";
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
    public static final String LARGEINT = "LARGEINT";
    public static final String BIGINT = "BIGINT";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String DECIMAL = "DECIMAL";

    // String
    public static final String CHAR = "CHAR";
    public static final String VARCHAR = "VARCHAR";
    public static final String STRING = "STRING";
    public static final String BINARY = "BINARY";
    public static final String VARBINARY = "VARBINARY";

    // Date
    public static final String DATE = "DATE";
    public static final String DATETIME = "DATETIME";

    // Semi-structured
    public static final String JSON = "JSON";
    public static final String ARRAY = "ARRAY";
    public static final String MAP = "MAP";
    public static final String STRUCT = "STRUCT";

    // Other
    public static final String BITMAP = "BITMAP";
    public static final String HLL = "HLL";

    /**
     * Max size of char type of StarRocks. It's the number of characters in OceanBase, and the
     * number of bytes in StarRocks. One chinese character will use 3 bytes because it uses UTF-8,
     * so the length of StarRocks varchar type should be three times as that of OceanBase's varchar
     * type.
     */
    public static final int MAX_CHAR_SIZE = 255;

    /** Max size of varchar type of StarRocks. */
    public static final int MAX_VARCHAR_SIZE = 1048576;

    private static final String SINGLE_PRECISION_FORMAT = "%s(%s)";
    private static final String DOUBLE_PRECISION_FORMAT = "%s(%s,%s)";

    public static String toOceanBaseMySQLType(OceanBaseColumn fieldSchema) {
        switch (fieldSchema.getTypeString().toUpperCase()) {
                // Numeric
            case BOOLEAN:
                return OceanBaseMySQLType.BOOLEAN;
            case TINYINT:
                return OceanBaseMySQLType.TINYINT;
            case SMALLINT:
                return OceanBaseMySQLType.SMALLINT;
            case INT:
                return OceanBaseMySQLType.INT;
            case BIGINT:
            case LARGEINT:
                return OceanBaseMySQLType.BIGINT;
            case FLOAT:
                return OceanBaseMySQLType.FLOAT;
            case DOUBLE:
                return OceanBaseMySQLType.DOUBLE;
            case DECIMAL:
                return String.format(
                        DOUBLE_PRECISION_FORMAT,
                        OceanBaseMySQLType.DECIMAL,
                        fieldSchema.getColumnSize(),
                        fieldSchema.getNumericScale());
                // String
            case CHAR:
                return String.format(
                        SINGLE_PRECISION_FORMAT,
                        OceanBaseMySQLType.CHAR,
                        fieldSchema.getColumnSize());
            case VARCHAR:
                Integer sRVarColumnSize = fieldSchema.getColumnSize();
                Preconditions.checkState(Objects.nonNull(sRVarColumnSize));

                // The actual character length of a Varchar cannot be determined and can only be
                // estimated using the maximum length.
                if (sRVarColumnSize > OceanBaseMySQLType.MAX_VARCHAR_SIZE) {
                    return OceanBaseMySQLType.MEDIUMTEXT;
                }

                return String.format(
                        SINGLE_PRECISION_FORMAT, OceanBaseMySQLType.VARCHAR, sRVarColumnSize);
            case STRING:
                return String.format(
                        SINGLE_PRECISION_FORMAT,
                        OceanBaseMySQLType.VARCHAR,
                        OceanBaseMySQLType.RE_VARCHAR_SIZE);
            case BINARY:
                return String.format(
                        SINGLE_PRECISION_FORMAT,
                        OceanBaseMySQLType.BINARY,
                        fieldSchema.getColumnSize());
            case VARBINARY:
                return String.format(
                        SINGLE_PRECISION_FORMAT,
                        OceanBaseMySQLType.VARBINARY,
                        fieldSchema.getColumnSize());

                // Date
            case DATE:
                return OceanBaseMySQLType.DATE;
            case DATETIME:
                // Since StarRocks cannot obtain precision information
                return OceanBaseMySQLType.DATETIME;

                // Semi-structured
            case STRUCT:
            case MAP:
                return String.format(
                        SINGLE_PRECISION_FORMAT,
                        OceanBaseMySQLType.VARCHAR,
                        OceanBaseMySQLType.RE_VARCHAR_SIZE);
            case JSON:
                return OceanBaseMySQLType.JSON;
            case ARRAY:
                int count = 0;
                int index = 0;
                String array =
                        fieldSchema
                                .getColumnType()
                                .toUpperCase()
                                .replace("<", "(")
                                .replace(">", ")");
                String target = "ARRAY";
                while ((index = array.indexOf(target, index)) != -1) {
                    count++;
                    index += target.length();
                }
                if (count <= 6) {
                    return array;
                } else {
                    return String.format(
                            SINGLE_PRECISION_FORMAT,
                            OceanBaseMySQLType.VARCHAR,
                            OceanBaseMySQLType.RE_VARCHAR_SIZE);
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported StarRocks type: " + fieldSchema.getTypeString());
        }
    }

    public static LogicalType toFlinkDataType(OceanBaseColumn fieldSchema) {
        switch (fieldSchema.getTypeString().toUpperCase()) {
                // Numeric
            case BOOLEAN:
                return new BooleanType();
            case TINYINT:
                return DataTypes.TINYINT().getLogicalType();
            case SMALLINT:
                return DataTypes.SMALLINT().getLogicalType();
            case INT:
                return DataTypes.INT().getLogicalType();
            case BIGINT:
            case LARGEINT:
                return DataTypes.BIGINT().getLogicalType();
            case FLOAT:
                return DataTypes.FLOAT().getLogicalType();
            case DOUBLE:
                return DataTypes.DOUBLE().getLogicalType();
            case DECIMAL:
                return DataTypes.DECIMAL(fieldSchema.getColumnSize(), fieldSchema.getNumericScale())
                        .getLogicalType();
                // String
            case CHAR:
            case VARCHAR:
            case STRING:
                return DataTypes.STRING().getLogicalType();
            case BINARY:
                return DataTypes.BINARY(fieldSchema.getColumnSize()).getLogicalType();
            case VARBINARY:
                return DataTypes.VARBINARY(fieldSchema.getColumnSize()).getLogicalType();

                // Date
            case DATE:
                return DataTypes.DATE().getLogicalType();
            case DATETIME:
                // Since StarRocks cannot obtain precision information
                return DataTypes.TIMESTAMP().getLogicalType();

                // Semi-structured
            case JSON:
                return DataTypes.STRING().getLogicalType();
            case ARRAY:
                return parseArrayType(fieldSchema.getColumnType().toUpperCase());
            case STRUCT:
                return DataTypes.ROW().getLogicalType();
            case MAP:
                String columnType = fieldSchema.getColumnType().toLowerCase();
                String[] split = columnType.substring(4, columnType.length() - 1).split(",");
                List<String> list =
                        Arrays.stream(split)
                                .map(sp -> sp.contains("(") ? sp.substring(0, sp.indexOf("(")) : sp)
                                .collect(Collectors.toList());
                OceanBaseColumn column1 =
                        OceanBaseColumn.FieldSchemaBuilder.aFieldSchema()
                                .withTypeString(list.get(0))
                                .build();
                OceanBaseColumn column2 =
                        OceanBaseColumn.FieldSchemaBuilder.aFieldSchema()
                                .withTypeString(list.get(1))
                                .build();
                return new MapType(toFlinkDataType(column1), toFlinkDataType(column2));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported StarRocks type: " + fieldSchema.getTypeString());
        }
    }

    public static LogicalType parseArrayType(String type) {
        if (!type.startsWith("ARRAY<") || !type.endsWith(">")) {
            throw new IllegalArgumentException(
                    "The provided type is not a valid ARRAY type: " + type);
        }
        int count = 0;
        String innerTypeString = type.substring(6, type.length() - 1).trim();
        while (innerTypeString.startsWith("ARRAY<") && innerTypeString.endsWith(">")) {
            innerTypeString = innerTypeString.substring(6, innerTypeString.length() - 1).trim();
            count++;
        }
        OceanBaseColumn oceanBaseColumn = new OceanBaseColumn();
        oceanBaseColumn.setTypeString(extractBaseType(innerTypeString));
        LogicalType dataType = toFlinkDataType(oceanBaseColumn);
        for (int i = 0; i <= count; i++) {
            dataType = new ArrayType(dataType);
        }
        return dataType;
    }

    public static String extractBaseType(String type) {
        int parenthesisIndex = type.indexOf('(');

        if (parenthesisIndex != -1) {
            return type.substring(0, parenthesisIndex).trim();
        } else {
            return type.trim();
        }
    }

    public static List<String> getStringBasedType() {
        return Arrays.asList(StarRocksType.CHAR, StarRocksType.VARCHAR, StarRocksType.STRING);
    }

    public static List<String> getDateBasedType() {
        return Arrays.asList(StarRocksType.DATE, StarRocksType.DATETIME);
    }
}
