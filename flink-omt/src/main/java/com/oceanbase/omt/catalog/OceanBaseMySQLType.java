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

public class OceanBaseMySQLType {

    // Numeric
    public static final String BOOLEAN = "BOOLEAN";
    public static final String TINYINT = "TINYINT";
    public static final String SMALLINT = "SMALLINT";
    public static final String MEDIUMINT = "MEDIUMINT";
    public static final String INT = "INT";
    public static final String BIGINT = "BIGINT";
    public static final String FLOAT = "FLOAT";
    public static final String DOUBLE = "DOUBLE";
    public static final String DECIMAL = "DECIMAL";
    public static final String BIT = "BIT";

    // String
    public static final String CHAR = "CHAR";
    public static final String VARCHAR = "VARCHAR";
    public static final String BINARY = "BINARY";
    public static final String VARBINARY = "VARBINARY";

    // Date
    public static final String DATE = "DATE";
    public static final String TIME = "TIME";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String YEAR = "YEAR";
    public static final String DATETIME = "DATETIME";

    // BLOB
    public static final String TINYBLOB = "TINYBLOB";
    public static final String BLOB = "BLOB";
    public static final String MEDIUMBLOB = "MEDIUMBLOB";
    public static final String LONGBLOB = "LONGBLOB";

    // TEXT
    public static final String TINYTEXT = "TINYTEXT";
    public static final String TEXT = "TEXT";
    public static final String MEDIUMTEXT = "MEDIUMTEXT";
    public static final String LONGTEXT = "LONGTEXT";

    // Semi-structured
    public static final String JSON = "JSON";
    public static final String SET = "SET";
    public static final String ENUM = "ENUM";

    /** Max size of char type of OceanBase. */
    public static final int MAX_CHAR_SIZE = 256;

    /** Max size of varchar type of OceanBase. */
    public static final int MAX_VARCHAR_SIZE = 262144;

    /** Avoid row size too large. */
    public static final int RE_VARCHAR_SIZE = 65535;

    /** The max VARBINARY column length is 1048576. */
    public static final int MAX_VARBINARY_SIZE = 1048576;

    public static final int MAX_TEXT_SIZE = 65536;

    public static final int MAX_MEDIUMTEXT_SIZE = 16777216;
}
