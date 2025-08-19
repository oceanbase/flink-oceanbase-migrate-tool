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

import com.oceanbase.omt.doris.DorisContainerTestBase;
import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.OBMigrateConfig;
import com.oceanbase.omt.parser.PipelineConfig;
import com.oceanbase.omt.parser.SourceMigrateConfig;
import com.oceanbase.omt.source.doris.DorisDatabaseSync;
import com.oceanbase.omt.utils.DataSourceUtils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Doris到OceanBase的Docker集成测试类 */
public class Doris2OBTest extends DorisContainerTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(Doris2OBTest.class);

    // 测试数据库名称
    private static final String TEST_DB_1 = "test1";
    private static final String TEST_DB_2 = "test2";

    @Before
    public void setUp() throws Exception {
        LOG.info("Setting up Doris2OB integration test...");

        // 创建测试数据库
        createDatabase(TEST_DB_1);
        createDatabase(TEST_DB_2);
        // 初始化测试数据
        initialize(getDorisConnection(), "sql/doris-sql.sql");

        LOG.info("Doris2OB integration test setup completed");
    }

    @After
    public void tearDown() throws Exception {
        LOG.info("Cleaning up Doris2OB integration test...");

        // 清理测试数据库
        try {
            dropDatabase(TEST_DB_1);
            dropDatabase(TEST_DB_2);
        } catch (Exception e) {
            LOG.warn("Failed to drop test databases: {}", e.getMessage());
        }

        LOG.info("Doris2OB integration test cleanup completed");
    }

    /** 测试JDBC方式的数据同步 */
    @Test
    public void testJdbcSync() throws Exception {
        LOG.info("Starting JDBC sync test...");

        // 创建测试配置
        MigrationConfig config = createJdbcConfig();

        // 执行数据同步
        executeDataSync(config);

        // 验证同步结果
        verifyJdbcSyncResults();

        LOG.info("JDBC sync test completed successfully");
    }

    /** 执行数据同步 */
    private void executeDataSync(MigrationConfig config) throws Exception {
        LOG.info("Executing data sync with config: {}", config.getPipeline().getName());

        // 创建Flink执行环境
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.enableCheckpointing(1000);

        // 创建Doris数据库同步对象
        DorisDatabaseSync dorisDatabaseSync = new DorisDatabaseSync(config);

        // 在OceanBase中创建表结构
        dorisDatabaseSync.createTableInOb();

        // 构建并执行Pipeline
        dorisDatabaseSync.buildPipeline(env);
        env.execute(config.getPipeline().getName());

        LOG.info("Data sync execution completed");
    }

    /** 创建JDBC配置 */
    private MigrationConfig createJdbcConfig() {
        SourceMigrateConfig sourceConfig = new SourceMigrateConfig();
        sourceConfig.setType("doris");
        sourceConfig.setOther("jdbc_url", DORIS_CONTAINER.getJdbcUrl());
        sourceConfig.setOther("username", DORIS_CONTAINER.getUsername());
        sourceConfig.setOther("password", DORIS_CONTAINER.getPassword());
        sourceConfig.setOther("fenodes", DORIS_CONTAINER.getFeNodes());
        sourceConfig.setTables("test[1-2].orders[0-9]");

        OBMigrateConfig obConfig = createOceanBaseConfig();

        PipelineConfig pipelineConfig = new PipelineConfig();
        pipelineConfig.setName("Doris2OB-JDBC-Test");
        pipelineConfig.setParallelism(1);

        MigrationConfig config = new MigrationConfig();
        config.setSource(sourceConfig);
        config.setOceanbase(obConfig);
        config.setPipeline(pipelineConfig);

        return config;
    }

    /** 创建OceanBase配置 */
    private OBMigrateConfig createOceanBaseConfig() {
        OBMigrateConfig obConfig = new OBMigrateConfig();
        obConfig.setUrl(
                "jdbc:mysql://"
                        + FIX_CONTAINER.getHost()
                        + ":"
                        + FIX_CONTAINER.getMappedPort(2881)
                        + "/test");
        obConfig.setUsername("root@test");
        obConfig.setPassword("654321");
        obConfig.setSchemaName("test");
        obConfig.setType("jdbc");
        return obConfig;
    }

    /** 验证JDBC同步结果 */
    private void verifyJdbcSyncResults() throws Exception {
        LOG.info("Verifying JDBC sync results...");

        DataSource dataSource = DataSourceUtils.getOBDataSource(createOceanBaseConfig());

        // 验证 test1.orders1
        List<String> expected1 = Collections.singletonList("1,2024-12-05 10:28:07,xx,2.3,1,1");
        assertContent(dataSource.getConnection(), expected1, "test1.orders1");

        // 验证 test1.orders2
        List<String> expected2 =
                Collections.singletonList("111,2024-12-05 10:02:31,orders2,2.3,1,1");
        assertContent(dataSource.getConnection(), expected2, "test1.orders2");

        // 验证 test2.orders4（复杂数据类型）
        List<String> expected3 =
                Arrays.asList(
                        "1,1,A,2023-01-01,2023-01-01 10:10:10,1234.5678,1.23456789,1.2345,123,12,example string 1,1,example varchar 1,{\"key1\": \"value1\"}",
                        "2,0,B,2023-02-01,2023-02-02 11:11:11,9876.5432,9.87654321,9.8765,456,34,example string 2,2,example varchar 2,{\"key2\": \"value2\"}",
                        "5,1,E,2023-05-01,2023-05-05 14:14:14,8765.4321,8.7654321,8.7654,202,90,example string 5,5,example varchar 5,{\"key5\": \"value5\"}",
                        "4,0,D,2023-04-01,2023-04-04 13:13:13,4321.8765,4.32187654,4.3211,101,78,example string 4,4,example varchar 4,{\"key4\": \"value4\"}",
                        "3,1,C,2023-03-01,2023-03-03 12:12:12,5678.1234,5.67812345,5.6789,789,56,example string 3,3,example varchar 3,{\"key3\": \"value3\"}");
        assertContent(dataSource.getConnection(), expected3, "test2.orders4");

        // 验证 test2.orders5（复杂类型）
        List<String> expected4 = Collections.singletonList("1,{a=100, b=200},[6,7,8]");
        assertContent(dataSource.getConnection(), expected4, "test2.orders5");

        LOG.info("JDBC sync results verification completed");
    }

    /** 验证路由同步结果 */
    private void verifyRouteSyncResults() throws Exception {
        LOG.info("Verifying route sync results...");

        DataSource dataSource = DataSourceUtils.getOBDataSource(createOceanBaseConfig());

        // 验证路由后的 test1.order1
        List<String> expected1 = Collections.singletonList("1,2024-12-05 10:28:07,xx,2.3,1,1");
        assertContent(dataSource.getConnection(), expected1, "test1.order1");

        // 验证路由后的 route.order（合并了orders2和orders3）
        List<String> expected2 =
                Arrays.asList(
                        "111,2024-12-05 10:02:31,orders2,2.3,1,1",
                        "11,2024-12-01 10:03:31,orders3-2-route,2.3,1,1",
                        "12,2024-12-02 10:02:35,orders3,2.3,1,1",
                        "10,2024-12-05 10:02:31,orders3,2.3,1,1");
        assertContent(dataSource.getConnection(), expected2, "route.order");

        // 验证 test2.orders4（复杂数据类型）
        List<String> expected3 =
                Arrays.asList(
                        "1,1,A,2023-01-01,2023-01-01 10:10:10,1234.5678,1.23456789,1.2345,123,12,example string 1,1,example varchar 1,{\"key1\": \"value1\"}",
                        "2,0,B,2023-02-01,2023-02-02 11:11:11,9876.5432,9.87654321,9.8765,456,34,example string 2,2,example varchar 2,{\"key2\": \"value2\"}",
                        "5,1,E,2023-05-01,2023-05-05 14:14:14,8765.4321,8.7654321,8.7654,202,90,example string 5,5,example varchar 5,{\"key5\": \"value5\"}",
                        "4,0,D,2023-04-01,2023-04-04 13:13:13,4321.8765,4.32187654,4.3211,101,78,example string 4,4,example varchar 4,{\"key4\": \"value4\"}",
                        "3,1,C,2023-03-01,2023-03-03 12:12:12,5678.1234,5.67812345,5.6789,789,56,example string 3,3,example varchar 3,{\"key3\": \"value3\"}");
        assertContent(dataSource.getConnection(), expected3, "test2.orders4");

        LOG.info("Route sync results verification completed");
    }

    /** 验证旁路导入结果 */
    private void verifyDirectLoadResults() throws Exception {
        LOG.info("Verifying direct load results...");

        DataSource dataSource = DataSourceUtils.getOBDataSource(createOceanBaseConfig());

        // 验证同步结果（与JDBC方式相同）
        verifyJdbcSyncResults();

        LOG.info("Direct load results verification completed");
    }

    /** 验证表内容 */
    private void assertContent(Connection connection, List<String> expected, String tableName)
            throws SQLException {
        List<String> actual = queryTable(connection, tableName);
        assertEqualsInAnyOrder(expected, actual);
    }
}
