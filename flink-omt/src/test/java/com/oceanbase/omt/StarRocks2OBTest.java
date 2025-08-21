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

import com.oceanbase.omt.base.OceanBaseMySQLTestBase;
import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.PipelineConfig;
import com.oceanbase.omt.parser.YamlParser;
import com.oceanbase.omt.source.starrocks.StarRocksDatabaseSync;
import com.oceanbase.omt.utils.DataSourceUtils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;

import javax.sql.DataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class StarRocks2OBTest extends OceanBaseMySQLTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocks2OBTest.class);
    private static final String SR_DOCKER_IMAGE_NAME = "starrocks/allin1-ubuntu:3.2.6";

    // exposed ports
    public static final int FE_HTTP_SERVICE_PORT = 8080;
    public static final int FE_QUERY_PORT = 9030;
    public static final int SCAN_PORT = 8030;
    public static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;

    public static final GenericContainer<?> STARROCKS_CONTAINER =
            new FixedHostPortGenericContainer<>(SR_DOCKER_IMAGE_NAME)
                    .withNetwork(NETWORK)
                    .withFixedExposedPort(FE_HTTP_SERVICE_PORT, FE_HTTP_SERVICE_PORT)
                    .withFixedExposedPort(FE_QUERY_PORT, FE_QUERY_PORT)
                    .withFixedExposedPort(SCAN_PORT, SCAN_PORT)
                    .withFixedExposedPort(9060, 9060)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        FIX_CONTAINER.waitingFor(
                new LogMessageWaitStrategy()
                        .withRegEx(".*boot success!.*")
                        .withTimes(1)
                        .withStartupTimeout(Duration.ofMinutes(6)));
        FIX_CONTAINER.start();

        Startables.deepStart(Stream.of(STARROCKS_CONTAINER)).join();
        LOG.info("Waiting for StarRocks to launch");

        long startWaitingTimestamp = System.currentTimeMillis();

        new LogMessageWaitStrategy()
                .withRegEx(".*Enjoy the journal to StarRocks blazing-fast lake-house engine!.*\\s")
                .withTimes(1)
                .withStartupTimeout(
                        Duration.of(DEFAULT_STARTUP_TIMEOUT_SECONDS, ChronoUnit.SECONDS))
                .waitUntilReady(STARROCKS_CONTAINER);

        while (!checkBackendAvailability()) {
            try {
                if (System.currentTimeMillis() - startWaitingTimestamp
                        > DEFAULT_STARTUP_TIMEOUT_SECONDS * 1000) {
                    throw new RuntimeException("StarRocks backend startup timed out.");
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
        STARROCKS_CONTAINER.stop();
        FIX_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    @Before
    public void init() throws IOException, SQLException {
        // 1. Parse config
        MigrationConfig migrationConfig = YamlParser.parseResource("config.yaml");
        DataSource sourceDataSource =
                DataSourceUtils.getSourceDataSource(migrationConfig.getSource());
        crateDataBases(sourceDataSource.getConnection(), "test1", "test2");
        initialize(sourceDataSource.getConnection(), "sql/starRocks-sql.sql");
    }

    @After
    public void close() throws IOException, SQLException {
        MigrationConfig migrationConfig = YamlParser.parseResource("config.yaml");
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
        MigrationConfig migrationConfig = YamlParser.parseResource("config.yaml");
        DataSource dataSource = DataSourceUtils.getOBDataSource(migrationConfig.getOceanbase());

        // 2. Obtain StarRocks MetaData
        StarRocksDatabaseSync starRocksDatabaseSync = new StarRocksDatabaseSync(migrationConfig);
        starRocksDatabaseSync.createTableInOb();
        starRocksDatabaseSync.buildPipeline(env);
        env.execute(migrationConfig.getPipeline().getName());

        // table1
        List<String> expected1 = Collections.singletonList("1,2024-12-05 10:28:07,xx,2.3,1,1");
        assertContent(dataSource.getConnection(), expected1, "test1.orders1");

        List<String> expected2 =
                Collections.singletonList("111,2024-12-05 10:02:31,orders2,2.3,1,1");
        assertContent(dataSource.getConnection(), expected2, "test1.orders2");

        List<String> expected3 =
                Arrays.asList(
                        "1,1,A123456789,2023-01-01,2023-01-01 10:10:10,1234.5678,1.23456789,1.2345,123,12,example string 1,1,example varchar 1,{\"key1\": \"value1\"}",
                        "2,0,B987654321,2023-02-01,2023-02-02 11:11:11,9876.5432,9.87654321,9.8765,456,34,example string 2,2,example varchar 2,{\"key2\": \"value2\"}",
                        "5,1,E019283746,2023-05-01,2023-05-05 14:14:14,8765.4321,8.7654321,8.7654,202,90,example string 5,5,example varchar 5,{\"key5\": \"value5\"}",
                        "4,0,D564738291,2023-04-01,2023-04-04 13:13:13,4321.8765,4.32187654,4.3211,101,78,example string 4,4,example varchar 4,{\"key4\": \"value4\"}",
                        "3,1,C102938475,2023-03-01,2023-03-03 12:12:12,5678.1234,5.67812345,5.6789,789,56,example string 3,3,example varchar 3,{\"key3\": \"value3\"}");
        assertContent(dataSource.getConnection(), expected3, "test2.orders4");
    }

    @Test
    public void testColumnStore() throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.enableCheckpointing(1000);
        // 1. Parse config
        MigrationConfig migrationConfig = YamlParser.parseResource("config-column-store.yaml");
        DataSource dataSource = DataSourceUtils.getOBDataSource(migrationConfig.getOceanbase());

        // 2. Obtain StarRocks MetaData
        StarRocksDatabaseSync starRocksDatabaseSync = new StarRocksDatabaseSync(migrationConfig);
        starRocksDatabaseSync.createTableInOb();
        starRocksDatabaseSync.buildPipeline(env);
        env.execute(migrationConfig.getPipeline().getName());

        // table1
        List<String> expected1 = Collections.singletonList("1,2024-12-05 10:28:07,xx,2.3,1,1");
        assertContent(dataSource.getConnection(), expected1, "test1.orders1");

        List<String> expected2 =
                Collections.singletonList("111,2024-12-05 10:02:31,orders2,2.3,1,1");
        assertContent(dataSource.getConnection(), expected2, "test1.orders2");

        List<String> expected3 =
                Arrays.asList(
                        "1,1,A123456789,2023-01-01,2023-01-01 10:10:10,1234.5678,1.23456789,1.2345,123,12,example string 1,1,example varchar 1,{\"key1\": \"value1\"}",
                        "2,0,B987654321,2023-02-01,2023-02-02 11:11:11,9876.5432,9.87654321,9.8765,456,34,example string 2,2,example varchar 2,{\"key2\": \"value2\"}",
                        "5,1,E019283746,2023-05-01,2023-05-05 14:14:14,8765.4321,8.7654321,8.7654,202,90,example string 5,5,example varchar 5,{\"key5\": \"value5\"}",
                        "4,0,D564738291,2023-04-01,2023-04-04 13:13:13,4321.8765,4.32187654,4.3211,101,78,example string 4,4,example varchar 4,{\"key4\": \"value4\"}",
                        "3,1,C102938475,2023-03-01,2023-03-03 12:12:12,5678.1234,5.67812345,5.6789,789,56,example string 3,3,example varchar 3,{\"key3\": \"value3\"}");
        assertContent(dataSource.getConnection(), expected3, "test2.orders4");
    }

    @Test
    public void testDirectLoad() throws Exception {
        // 1. Parse config
        MigrationConfig migrationConfig = YamlParser.parseResource("config-directload.yaml");
        DataSource dataSource = DataSourceUtils.getOBDataSource(migrationConfig.getOceanbase());

        PipelineConfig pipeline = migrationConfig.getPipeline();
        String jobName = "null";
        Integer parallelism = 2;
        if (Objects.nonNull(pipeline)) {
            if (Objects.nonNull(pipeline.getParallelism())) {
                parallelism = pipeline.getParallelism();
            }
            jobName = pipeline.getName();
        }
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(parallelism);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.enableCheckpointing(1000);

        // 2. Obtain StarRocks MetaData
        StarRocksDatabaseSync starRocksDatabaseSync = new StarRocksDatabaseSync(migrationConfig);
        starRocksDatabaseSync.createTableInOb();
        starRocksDatabaseSync.buildPipeline(env);
        env.execute(jobName);

        // table1
        List<String> expected1 = Collections.singletonList("1,2024-12-05 10:28:07,xx,2.3,1,1");
        assertContent(dataSource.getConnection(), expected1, "test1.orders1");

        List<String> expected3 =
                Arrays.asList(
                        "1,1,A123456789,2023-01-01,2023-01-01 10:10:10,1234.5678,1.23456789,1.2345,123,12,example string 1,1,example varchar 1,{\"key1\": \"value1\"}",
                        "2,0,B987654321,2023-02-01,2023-02-02 11:11:11,9876.5432,9.87654321,9.8765,456,34,example string 2,2,example varchar 2,{\"key2\": \"value2\"}",
                        "5,1,E019283746,2023-05-01,2023-05-05 14:14:14,8765.4321,8.7654321,8.7654,202,90,example string 5,5,example varchar 5,{\"key5\": \"value5\"}",
                        "4,0,D564738291,2023-04-01,2023-04-04 13:13:13,4321.8765,4.32187654,4.3211,101,78,example string 4,4,example varchar 4,{\"key4\": \"value4\"}",
                        "3,1,C102938475,2023-03-01,2023-03-03 12:12:12,5678.1234,5.67812345,5.6789,789,56,example string 3,3,example varchar 3,{\"key3\": \"value3\"}");
        assertContent(dataSource.getConnection(), expected3, "test2.orders4");
    }

    private void assertContent(Connection connection, List<String> expected, String tableName)
            throws SQLException {
        List<String> actual = queryTable(connection, tableName);
        assertEqualsInAnyOrder(expected, actual);
    }

    @Test
    public void testJdbcWithRoute() throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.enableCheckpointing(1000);
        // 1. Parse config
        MigrationConfig migrationConfig = YamlParser.parseResource("config-with-route.yaml");
        DataSource dataSource = DataSourceUtils.getOBDataSource(migrationConfig.getOceanbase());

        // 2. Obtain StarRocks MetaData
        StarRocksDatabaseSync starRocksDatabaseSync = new StarRocksDatabaseSync(migrationConfig);
        starRocksDatabaseSync.createTableInOb();
        starRocksDatabaseSync.buildPipeline(env);
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
                        "1,1,A123456789,2023-01-01,2023-01-01 10:10:10,1234.5678,1.23456789,1.2345,123,12,example string 1,1,example varchar 1,{\"key1\": \"value1\"}",
                        "2,0,B987654321,2023-02-01,2023-02-02 11:11:11,9876.5432,9.87654321,9.8765,456,34,example string 2,2,example varchar 2,{\"key2\": \"value2\"}",
                        "5,1,E019283746,2023-05-01,2023-05-05 14:14:14,8765.4321,8.7654321,8.7654,202,90,example string 5,5,example varchar 5,{\"key5\": \"value5\"}",
                        "4,0,D564738291,2023-04-01,2023-04-04 13:13:13,4321.8765,4.32187654,4.3211,101,78,example string 4,4,example varchar 4,{\"key4\": \"value4\"}",
                        "3,1,C102938475,2023-03-01,2023-03-03 12:12:12,5678.1234,5.67812345,5.6789,789,56,example string 3,3,example varchar 3,{\"key3\": \"value3\"}");
        assertContent(dataSource.getConnection(), expected3, "test2.orders4");

        // Do clean
        dropDataBases(dataSource.getConnection(), "route");
    }

    public static boolean checkBackendAvailability() {
        try {
            Container.ExecResult rs =
                    STARROCKS_CONTAINER.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-uroot",
                            "-P9030",
                            "-h127.0.0.1",
                            "-e SHOW BACKENDS\\G");

            if (rs.getExitCode() != 0) {
                return false;
            }
            return rs.getStdout()
                    .contains("*************************** 1. row ***************************");
        } catch (Exception e) {
            LOG.info("Failed to check backend status.", e);
            return false;
        }
    }
}
