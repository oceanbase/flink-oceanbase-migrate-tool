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

/** Doris configuration interface, defines configuration parameters for Doris data source */
public interface DorisConfig {
    String JDBC_URL = "jdbc-url";
    /** FE node HTTP service address, used to access FE node through web server */
    String FE_NODES = "fenodes";

    String BE_NODES = "benodes";

    /** FE node JDBC connection address, used to access MySQL client on FE node */
    String TABLE_IDENTIFIER = "table.identifier";

    /** Username for accessing Doris cluster */
    String USERNAME = "username";

    /** Password for accessing Doris cluster */
    String PASSWORD = "password";
}
