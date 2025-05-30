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
package com.oceanbase.omt.catalog;

/**
 * see <a
 * href="https://nightlies.apache.org/flink/flink-docs-master/zh/docs/dev/table/types/">...</a>
 */
public class FlinkSqlType {

    // Numeric
    public static final String BOOLEAN = "BOOLEAN";
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String INT = "INT";
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
    public static final String TIME = "TIME";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String TIMESTAMP_LTZ = "TIMESTAMP_LTZ";
    public static final String DATE = "DATE";

    // Semi-structured
    public static final String ARRAY = "ARRAY";
    public static final String MAP = "MAP";
    public static final String MULTISET = "MULTISET";
    public static final String ROW = "ROW";
    public static final String RAW = "RAW";
}
