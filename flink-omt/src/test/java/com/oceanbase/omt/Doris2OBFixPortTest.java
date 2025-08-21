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
import com.oceanbase.omt.parser.YamlParser;
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
import java.util.stream.Stream;

/** Docker integration test class for Doris to OceanBase */
@Ignore
public class Doris2OBFixPortTest extends OceanBaseMySQLTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(Doris2OBFixPortTest.class);

    private static final String DORIS_JDBC_YAML = "yaml/doris/config-doris.yaml";
    private static final String DORIS_JDBC_ROUTE_YAML = "yaml/doris/config-doris-route.yaml";
    private static final String DORIS_DIRECT_LOAD_YAML = "yaml/doris/config-doris-directload.yaml";

    // exposed ports
    public static final int FE_HTTP_SERVICE_PORT = 8040;
    public static final int FE_QUERY_PORT = 9030;
    public static final int SCAN_PORT = 8030;
    public static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;

    public static final GenericContainer<?> FIX_DORIS_CONTAINER =
            new FixedHostPortGenericContainer<>("apache/doris:doris-all-in-one-2.1.0")
                    .withNetwork(NETWORK)
                    .withFixedExposedPort(7040, FE_HTTP_SERVICE_PORT)
                    .withFixedExposedPort(7030, FE_QUERY_PORT)
                    .withFixedExposedPort(7031, SCAN_PORT)
                    .withFixedExposedPort(9060, 9060)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        FIX_CONTAINER.withFixedExposedPort(2771, 2881).withFixedExposedPort(2772, 2882);
        FIX_CONTAINER.waitingFor(
                new LogMessageWaitStrategy()
                        .withRegEx(".*boot success!.*")
                        .withTimes(1)
                        .withStartupTimeout(Duration.ofMinutes(6)));
        FIX_CONTAINER.start();

        Startables.deepStart(Stream.of(FIX_DORIS_CONTAINER)).join();
        LOG.info("Waiting for backends to be available");
        long startWaitingTimestamp = System.currentTimeMillis();

        new LogMessageWaitStrategy()
                .withRegEx(".*get heartbeat from FE.*\\s")
                .withTimes(1)
                .withStartupTimeout(
                        Duration.of(DEFAULT_STARTUP_TIMEOUT_SECONDS, ChronoUnit.SECONDS))
                .waitUntilReady(FIX_DORIS_CONTAINER);

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
        FIX_DORIS_CONTAINER.stop();
        FIX_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

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

        // 2. Obtain doris MetaData
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

    public static boolean checkBackendAvailability() {
        try {
            Container.ExecResult rs =
                    FIX_DORIS_CONTAINER.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-uroot",
                            "-P9030",
                            "-h127.0.0.1",
                            "-e SHOW BACKENDS\\G");

            if (rs.getExitCode() != 0) {
                return false;
            }
            String output = rs.getStdout();
            LOG.info("Doris backend status:\n{}", output);
            return output.contains("*************************** 1. row ***************************")
                    && !output.contains("AvailCapacity: 1.000 B");
        } catch (Exception e) {
            LOG.info("Failed to check backend status.", e);
            return false;
        }
    }
}
