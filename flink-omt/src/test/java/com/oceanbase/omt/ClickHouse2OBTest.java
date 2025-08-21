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
import com.oceanbase.omt.base.OceanBaseSingleton;
import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.OBMigrateConfig;
import com.oceanbase.omt.parser.YamlParser;
import com.oceanbase.omt.source.clickhouse.ClickHouseDatabaseSync;
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
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import javax.sql.DataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class ClickHouse2OBTest extends OceanBaseMySQLTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouse2OBTest.class);
    private static final GenericContainer<?> OB_CONTAINER = OceanBaseSingleton.getInstance();
    private static final String CLICKHOUSE_DOCKER_IMAGE_NAME =
            "clickhouse/clickhouse-server:latest";
    private static final String CLICKHOUSE_USER = "root";
    private static final String CLICKHOUSE_PASSWORD = "123456";

    private MigrationConfig migrationConfig;

    public static final GenericContainer<?> CLICKHOUSE_CONTAINER =
            new FixedHostPortGenericContainer<>(CLICKHOUSE_DOCKER_IMAGE_NAME)
                    .withNetwork(NETWORK)
                    .withFixedExposedPort(8123, 8123)
                    .withFixedExposedPort(9000, 9000)
                    .withEnv("CLICKHOUSE_PASSWORD", "123456")
                    .withEnv("CLICKHOUSE_USER", "root")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startContainers() throws SQLException, IOException, InterruptedException {
        LOG.info("Starting containers...");
        CLICKHOUSE_CONTAINER.start();
        Thread.sleep(120000);
        verifyClickHouseConnection();
        LOG.info("Containers are started.");
    }

    private static void verifyClickHouseConnection() throws SQLException {
        String jdbcUrl =
                String.format(
                        "jdbc:clickhouse://localhost:8123/default?user=%s&password=%s",
                        CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);

        try (Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT version()")) {
            if (rs.next()) {
                LOG.info("ClickHouse version: {}", rs.getString(1));
            } else {
                throw new SQLException("ClickHouse version query failed");
            }
        }
    }

    @AfterClass
    public static void stopContainers() throws SQLException, IOException {
        LOG.info("Stopping containers...");
        CLICKHOUSE_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    @Before
    public void init() throws IOException, SQLException {
        // 1. Parse config
        MigrationConfig migrationConfig = YamlParser.parseResource("clickhouse.yaml");
        DataSource sourceDataSource =
                DataSourceUtils.getSourceDataSource(migrationConfig.getSource());
        crateDataBases(sourceDataSource.getConnection(), "test1");
        initialize(sourceDataSource.getConnection(), "sql/clickHouse-sql.sql");
        Connection connection = sourceDataSource.getConnection();
        PreparedStatement preparedStatement =
                connection.prepareStatement("select * from test1.orders1");
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            System.out.println(
                    resultSet.getString(1)
                            + ","
                            + resultSet.getString(2)
                            + ","
                            + resultSet.getString(3)
                            + ","
                            + resultSet.getString(4)
                            + ","
                            + resultSet.getString(5)
                            + ","
                            + resultSet.getString(6));
        }
    }

    @After
    public void close() throws IOException, SQLException {
        migrationConfig = YamlParser.parseResource("clickhouse.yaml");
        // drop ob
        DataSource dataSource = DataSourceUtils.getOBDataSource(migrationConfig.getOceanbase());
        dropDataBases(dataSource.getConnection(), "test1");
        // drop source
        DataSource sourceDataSource =
                DataSourceUtils.getSourceDataSource(migrationConfig.getSource());
        dropDataBases(sourceDataSource.getConnection(), "test1");
    }

    @Test
    public void testJdbc() throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.enableCheckpointing(1000);
        migrationConfig = YamlParser.parseResource("clickhouse.yaml");
        OBMigrateConfig oceanbase = migrationConfig.getOceanbase();
        oceanbase.setUrl(
                "jdbc:mysql://"
                        + OB_CONTAINER.getHost()
                        + ":"
                        + OB_CONTAINER.getMappedPort(2881)
                        + "/mysql");
        migrationConfig.setOceanbase(oceanbase);
        DataSource dataSource = DataSourceUtils.getOBDataSource(migrationConfig.getOceanbase());
        ClickHouseDatabaseSync clickHouseDatabaseSync = new ClickHouseDatabaseSync(migrationConfig);
        clickHouseDatabaseSync.createTableInOb();
        clickHouseDatabaseSync.buildPipeline(env);
        env.execute(migrationConfig.getPipeline().getName());
        // table1
        List<String> expected1 =
                Arrays.asList(
                        "1,2025-06-01 10:00:00,Alice,199.99,101,0",
                        "2,2025-06-01 10:05:00,Bob,299.99,102,0");
        assertContent(dataSource.getConnection(), expected1, "test1.orders1");
    }

    private void assertContent(Connection connection, List<String> expected, String tableName)
            throws SQLException {
        List<String> actual = queryTable(connection, tableName);
        System.out.println(actual);
        assertEqualsInAnyOrder(expected, actual);
    }
}
