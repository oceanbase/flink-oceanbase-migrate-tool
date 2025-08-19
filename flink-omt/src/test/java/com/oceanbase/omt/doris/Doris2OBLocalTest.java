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

import com.oceanbase.omt.base.OceanBaseMySQLTestBase;
import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.YamlParser;
import com.oceanbase.omt.source.doris.DorisDatabaseSync;
import com.oceanbase.omt.utils.DataSourceUtils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.sql.DataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Doris到OceanBase的集成测试类 */
@Ignore
public class Doris2OBLocalTest extends OceanBaseMySQLTestBase {

    private static final String DORIS_JDBC_YAML = "yaml/doris/config-doris-test.yaml";
    private static final String DORIS_JDBC_ROUTE_YAML = "yaml/doris/config-doris-route-test.yaml";
    private static final String DORIS_DIRECT_LOAD_YAML =
            "yaml/doris/config-doris-direct-load-test.yaml";

    @Before
    public void init() throws IOException, SQLException {
        // 1. Parse config
        MigrationConfig migrationConfig = YamlParser.parseResource(DORIS_JDBC_YAML);
        DataSource sourceDataSource =
                DataSourceUtils.getSourceDataSource(migrationConfig.getSource());
        crateDataBases(sourceDataSource.getConnection(), "test1", "test2");
        initialize(sourceDataSource.getConnection(), "sql/doris-sql.sql");
    }

    @After
    public void close() throws IOException, SQLException {
        MigrationConfig migrationConfig = YamlParser.parseResource(DORIS_JDBC_YAML);
        // drop ob
        DataSource dataSource = DataSourceUtils.getOBDataSource(migrationConfig.getOceanbase());
        dropDataBases(dataSource.getConnection(), "test1", "test2");

        // drop source
        DataSource sourceDataSource =
                DataSourceUtils.getSourceDataSource(migrationConfig.getSource());
        dropDataBases(sourceDataSource.getConnection(), "test1", "test2");
    }

    @Test
    public void testJdbc() throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.enableCheckpointing(1000);
        // 1. Parse config
        MigrationConfig migrationConfig = YamlParser.parseResource(DORIS_JDBC_YAML);
        DataSource dataSource = DataSourceUtils.getOBDataSource(migrationConfig.getOceanbase());

        // 2. Obtain Doris MetaData
        DorisDatabaseSync dorisDatabaseSync = new DorisDatabaseSync(migrationConfig);
        dorisDatabaseSync.createTableInOb();
        dorisDatabaseSync.buildPipeline(env);
        env.execute(migrationConfig.getPipeline().getName());

        // table1
        List<String> expected1 = Collections.singletonList("1,2024-12-05 10:28:07,xx,2.3,1,1");
        assertContent(dataSource.getConnection(), expected1, "test1.orders1");

        List<String> expected2 =
                Collections.singletonList("111,2024-12-05 10:02:31,orders2,2.3,1,1");
        assertContent(dataSource.getConnection(), expected2, "test1.orders2");

        List<String> expected3 =
                Arrays.asList(
                        "1,1,A,2023-01-01,2023-01-01 10:10:10,1234.5678,1.23456789,1.2345,123,12,example string 1,1,example varchar 1,{\"key1\": \"value1\"}",
                        "2,0,B,2023-02-01,2023-02-02 11:11:11,9876.5432,9.87654321,9.8765,456,34,example string 2,2,example varchar 2,{\"key2\": \"value2\"}",
                        "5,1,E,2023-05-01,2023-05-05 14:14:14,8765.4321,8.7654321,8.7654,202,90,example string 5,5,example varchar 5,{\"key5\": \"value5\"}",
                        "4,0,D,2023-04-01,2023-04-04 13:13:13,4321.8765,4.32187654,4.3211,101,78,example string 4,4,example varchar 4,{\"key4\": \"value4\"}",
                        "3,1,C,2023-03-01,2023-03-03 12:12:12,5678.1234,5.67812345,5.6789,789,56,example string 3,3,example varchar 3,{\"key3\": \"value3\"}");
        assertContent(dataSource.getConnection(), expected3, "test2.orders4");

        List<String> expected4 = Collections.singletonList("1,{a=100, b=200},[6,7,8]");
        assertContent(dataSource.getConnection(), expected4, "test2.orders5");
    }

    @Test
    public void testJdbcWithRoute() throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.enableCheckpointing(1000);
        // 1. Parse config
        MigrationConfig migrationConfig = YamlParser.parseResource(DORIS_JDBC_ROUTE_YAML);
        DataSource dataSource = DataSourceUtils.getOBDataSource(migrationConfig.getOceanbase());

        // 2. Obtain Doris MetaData
        DorisDatabaseSync dorisDatabaseSync = new DorisDatabaseSync(migrationConfig);
        dorisDatabaseSync.createTableInOb();
        dorisDatabaseSync.buildPipeline(env);
        env.execute(migrationConfig.getPipeline().getName());

        // table1
        List<String> expected1 = Collections.singletonList("1,2024-12-05 10:28:07,xx,2.3,1,1");
        assertContent(dataSource.getConnection(), expected1, "test1.order1");

        List<String> expected2 =
                Arrays.asList(
                        "111,2024-12-05 10:02:31,orders2,2.3,1,1",
                        "11,2024-12-01 10:03:31,orders3-2-route,2.3,1,1",
                        "12,2024-12-02 10:02:35,orders3,2.3,1,1",
                        "10,2024-12-05 10:02:31,orders3,2.3,1,1");
        assertContent(dataSource.getConnection(), expected2, "route.order");

        List<String> expected3 =
                Arrays.asList(
                        "1,1,A,2023-01-01,2023-01-01 10:10:10,1234.5678,1.23456789,1.2345,123,12,example string 1,1,example varchar 1,{\"key1\": \"value1\"}",
                        "2,0,B,2023-02-01,2023-02-02 11:11:11,9876.5432,9.87654321,9.8765,456,34,example string 2,2,example varchar 2,{\"key2\": \"value2\"}",
                        "5,1,E,2023-05-01,2023-05-05 14:14:14,8765.4321,8.7654321,8.7654,202,90,example string 5,5,example varchar 5,{\"key5\": \"value5\"}",
                        "4,0,D,2023-04-01,2023-04-04 13:13:13,4321.8765,4.32187654,4.3211,101,78,example string 4,4,example varchar 4,{\"key4\": \"value4\"}",
                        "3,1,C,2023-03-01,2023-03-03 12:12:12,5678.1234,5.67812345,5.6789,789,56,example string 3,3,example varchar 3,{\"key3\": \"value3\"}");
        assertContent(dataSource.getConnection(), expected3, "test2.orders4");

        // Do clean
        dropDataBases(dataSource.getConnection(), "route");
    }

    @Ignore
    public void testDirectLoad() throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.enableCheckpointing(1000);
        // 1. Parse config
        MigrationConfig migrationConfig = YamlParser.parseResource(DORIS_DIRECT_LOAD_YAML);
        DataSource dataSource = DataSourceUtils.getOBDataSource(migrationConfig.getOceanbase());

        // 2. Obtain Doris MetaData
        DorisDatabaseSync dorisDatabaseSync = new DorisDatabaseSync(migrationConfig);
        dorisDatabaseSync.createTableInOb();
        dorisDatabaseSync.buildPipeline(env);
        env.execute(migrationConfig.getPipeline().getName());

        // table1
        List<String> expected1 = Collections.singletonList("1,2024-12-05 10:28:07,xx,2.3,1,1");
        assertContent(dataSource.getConnection(), expected1, "test1.orders1");

        List<String> expected2 =
                Collections.singletonList("111,2024-12-05 10:02:31,orders2,2.3,1,1");
        assertContent(dataSource.getConnection(), expected2, "test1.orders2");

        List<String> expected3 =
                Arrays.asList(
                        "1,1,A,2023-01-01,2023-01-01 10:10:10,1234.5678,1.23456789,1.2345,123,12,example string 1,1,example varchar 1,{\"key1\": \"value1\"}",
                        "2,0,B,2023-02-01,2023-02-02 11:11:11,9876.5432,9.87654321,9.8765,456,34,example string 2,2,example varchar 2,{\"key2\": \"value2\"}",
                        "5,1,E,2023-05-01,2023-05-05 14:14:14,8765.4321,8.7654321,8.7654,202,90,example string 5,5,example varchar 5,{\"key5\": \"value5\"}",
                        "4,0,D,2023-04-01,2023-04-04 13:13:13,4321.8765,4.32187654,4.3211,101,78,example string 4,4,example varchar 4,{\"key4\": \"value4\"}",
                        "3,1,C,2023-03-01,2023-03-03 12:12:12,5678.1234,5.67812345,5.6789,789,56,example string 3,3,example varchar 3,{\"key3\": \"value3\"}");
        assertContent(dataSource.getConnection(), expected3, "test2.orders4");
    }

    private void assertContent(Connection connection, List<String> expected, String tableName)
            throws SQLException {
        List<String> actual = queryTable(connection, tableName);
        assertEqualsInAnyOrder(expected, actual);
    }
}
