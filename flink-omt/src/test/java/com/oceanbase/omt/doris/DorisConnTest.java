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
package com.oceanbase.omt.doris;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for Doris to OceanBase migration.
 *
 * <p>Based on Flink CDC best practices and TestContainers patterns.
 */
public class DorisConnTest {

    private static final Logger LOG = LoggerFactory.getLogger(DorisConnTest.class);

    /**
     * CREATE TABLE IF NOT EXISTS test.t1 ( id int, map_col MAP<STRING, STRING> COMMENT 'Map type
     * example, stores key-value pairs', array_col ARRAY<int> COMMENT 'Array type example, stores a
     * list of values', variant_col VARIANT COMMENT 'Null type example, stores null values',
     * ipv4_col IPV4 , ipv6_col IPV6 COMMENT 'IPV6 type example, stores IPv6 addresses' )
     * ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) PROPERTIES ( "replication_num" =
     * "1" );
     *
     * <p>INSERT INTO test.t1 VALUES (1, {'a': '100', 'b': '200'},[6,7,8],'{"key1":
     * "value1"}','127.0.0.1', '2001:16a0:2:200a::2');
     *
     * <p>除了 variant_col 类型，其余可以正确读取 118801 [Source Data Fetcher for Source: dim_city[1] -> Sink:
     * Collect table sink (1/1)#0] WARN org.apache.doris.flink.backend.BackendClient [] - The status
     * of get next result from Doris BE{host='127.0.0.1', port=9060} is 'INTERNAL_ERROR', error
     * message is: [(127.0.0.1)[INTERNAL_ERROR]Fail to convert block data to arrow data, error: [E3]
     * write_column_to_arrow with type variant
     *
     * @throws Exception
     */
    @Test
    public void testFlinkEnvironmentSetup() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        tableEnvironment
                .executeSql(
                        "create table dim_city(\n"
                                + "  `id` INT,\n"
                                + "  `map_col` STRING,\n"
                                + "  `array_col` STRING, \n"
                                +
                                //                "  `variant_col` STRING,\n" +
                                "  `ipv4_col` STRING\n"
                                +
                                //                "  `ipv6_col` STRING\n" +
                                ") WITH (\n"
                                + "  'connector' = 'doris',\n"
                                + "  'fenodes' = '127.0.0.1:8030',\n"
                                + "  'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',\n"
                                + "  'table.identifier' = 'test.t4',\n"
                                + "  'username' = 'root',\n"
                                + "  'password' = ''\n"
                                + ");")
                .print();

        tableEnvironment.executeSql("Select * from dim_city").print();
    }
}
