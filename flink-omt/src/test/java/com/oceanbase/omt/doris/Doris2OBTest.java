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

import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.OBMigrateConfig;
import com.oceanbase.omt.parser.PipelineConfig;
import com.oceanbase.omt.parser.SourceMigrateConfig;
import com.oceanbase.omt.source.doris.DorisConfig;
import com.oceanbase.omt.source.doris.DorisDatabaseSync;
import com.oceanbase.omt.utils.DataSourceUtils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/** Docker integration test class for Doris to OceanBase */
@Ignore
public class Doris2OBTest extends DorisContainerTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(Doris2OBTest.class);

    // Test database names
    private static final String TEST_DB_1 = "test1";
    private static final String TEST_DB_2 = "test2";

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        OB_CONTAINER.waitingFor(
                new LogMessageWaitStrategy()
                        .withRegEx(".*boot success!.*")
                        .withTimes(1)
                        .withStartupTimeout(Duration.ofMinutes(6)));
        OB_CONTAINER.start();

        Startables.deepStart(Stream.of(DORIS_CONTAINER)).join();
        LOG.info("Waiting for backends to be available");
        long startWaitingTimestamp = System.currentTimeMillis();

        new LogMessageWaitStrategy()
                .withRegEx(".*get heartbeat from FE.*\\s")
                .withTimes(1)
                .withStartupTimeout(
                        Duration.of(DEFAULT_STARTUP_TIMEOUT_SECONDS, ChronoUnit.SECONDS))
                .waitUntilReady(DORIS_CONTAINER);

        while (!checkBackendAvailability()) {
            try {
                if (System.currentTimeMillis() - startWaitingTimestamp
                        > DEFAULT_STARTUP_TIMEOUT_SECONDS * 1000) {
                    throw new RuntimeException("Doris backend startup timed out.");
                }
                LOG.info("Waiting for backends to be available");
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                // ignore and check next round
            }
        }

        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        OB_CONTAINER.stop();
        DORIS_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    @Before
    public void setUp() throws Exception {
        LOG.info("Setting up Doris2OB integration test...");

        // Create test databases
        createDatabase(TEST_DB_1);
        createDatabase(TEST_DB_2);
        // Initialize test data
        initialize(getDorisConnection(), "sql/doris-sql.sql");

        LOG.info("Doris2OB integration test setup completed");
    }

    @After
    public void tearDown() throws Exception {
        LOG.info("Cleaning up Doris2OB integration test...");

        // Clean up test databases
        try {
            dropDatabase(TEST_DB_1);
            dropDatabase(TEST_DB_2);
        } catch (Exception e) {
            LOG.warn("Failed to drop test databases: {}", e.getMessage());
        }

        LOG.info("Doris2OB integration test cleanup completed");
    }

    /** Test JDBC data synchronization */
    @Test
    public void testJdbcSync() throws Exception {
        LOG.info("Starting JDBC sync test...");

        // Create test configuration
        MigrationConfig config = createJdbcConfig();

        // Execute data synchronization
        executeDataSync(config);

        // Verify synchronization results
        verifyJdbcSyncResults();

        LOG.info("JDBC sync test completed successfully");
    }

    /** Execute data synchronization */
    private void executeDataSync(MigrationConfig config) throws Exception {
        LOG.info("Executing data sync with config: {}", config.getPipeline().getName());

        // Create Flink execution environment
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.enableCheckpointing(1000);

        // Create Doris database synchronization object
        DorisDatabaseSync dorisDatabaseSync = new DorisDatabaseSync(config);

        // Create table structure in OceanBase
        dorisDatabaseSync.createTableInOb();

        // Build and execute Pipeline
        dorisDatabaseSync.buildPipeline(env);
        env.execute(config.getPipeline().getName());

        LOG.info("Data sync execution completed");
    }

    /** Create JDBC configuration */
    private MigrationConfig createJdbcConfig() {
        SourceMigrateConfig sourceConfig = new SourceMigrateConfig();
        sourceConfig.setType("doris");
        sourceConfig.setOther(DorisConfig.JDBC_URL, DORIS_CONTAINER.getJdbcUrl());
        sourceConfig.setOther("username", DORIS_CONTAINER.getUsername());
        sourceConfig.setOther("password", DORIS_CONTAINER.getPassword());
        sourceConfig.setOther("fenodes", DORIS_CONTAINER.getFeNodes());
        sourceConfig.setOther(DorisConfig.BE_NODES, DORIS_CONTAINER.getBeNodes());
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

    /** Create OceanBase configuration */
    private OBMigrateConfig createOceanBaseConfig() {
        OBMigrateConfig obConfig = new OBMigrateConfig();
        obConfig.setUrl(
                "jdbc:mysql://"
                        + OB_CONTAINER.getHost()
                        + ":"
                        + OB_CONTAINER.getMappedPort(2881)
                        + "/test");
        obConfig.setUsername("root@test");
        obConfig.setPassword("");
        obConfig.setSchemaName("test");
        obConfig.setType("jdbc");
        return obConfig;
    }

    /** Verify JDBC synchronization results */
    private void verifyJdbcSyncResults() throws Exception {
        LOG.info("Verifying JDBC sync results...");

        DataSource dataSource = DataSourceUtils.getOBDataSource(createOceanBaseConfig());

        // Verify test1.orders1
        List<String> expected1 = Collections.singletonList("1,2024-12-05 10:28:07,xx,2.3,1,1");
        assertContent(dataSource.getConnection(), expected1, "test1.orders1");

        // Verify test1.orders2
        List<String> expected2 =
                Collections.singletonList("111,2024-12-05 10:02:31,orders2,2.3,1,1");
        assertContent(dataSource.getConnection(), expected2, "test1.orders2");

        // Verify test2.orders4 (complex data types)
        List<String> expected3 =
                Arrays.asList(
                        "1,1,A,2023-01-01,2023-01-01 10:10:10,1234.5678,1.23456789,1.2345,123,12,example string 1,1,example varchar 1,{\"key1\": \"value1\"}",
                        "2,0,B,2023-02-01,2023-02-02 11:11:11,9876.5432,9.87654321,9.8765,456,34,example string 2,2,example varchar 2,{\"key2\": \"value2\"}",
                        "5,1,E,2023-05-01,2023-05-05 14:14:14,8765.4321,8.7654321,8.7654,202,90,example string 5,5,example varchar 5,{\"key5\": \"value5\"}",
                        "4,0,D,2023-04-01,2023-04-04 13:13:13,4321.8765,4.32187654,4.3211,101,78,example string 4,4,example varchar 4,{\"key4\": \"value4\"}",
                        "3,1,C,2023-03-01,2023-03-03 12:12:12,5678.1234,5.67812345,5.6789,789,56,example string 3,3,example varchar 3,{\"key3\": \"value3\"}");
        assertContent(dataSource.getConnection(), expected3, "test2.orders4");

        // Verify test2.orders5 (complex types)
        List<String> expected4 = Collections.singletonList("1,{a=100, b=200},[6,7,8]");
        assertContent(dataSource.getConnection(), expected4, "test2.orders5");

        LOG.info("JDBC sync results verification completed");
    }

    /** Verify table content */
    private void assertContent(Connection connection, List<String> expected, String tableName)
            throws SQLException {
        List<String> actual = queryTable(connection, tableName);
        assertEqualsInAnyOrder(expected, actual);
    }
}
