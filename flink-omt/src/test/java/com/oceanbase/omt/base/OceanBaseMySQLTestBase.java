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

package com.oceanbase.omt.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class OceanBaseMySQLTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseMySQLTestBase.class);

    private static final int SQL_PORT = 2881;
    private static final int RPC_PORT = 2882;
    private static final int CONFIG_SERVER_PORT = 8080;
    private static final String CONFIG_URL_PATH = "/services?Action=GetObProxyConfig";

    private static final String CLUSTER_NAME = "spark-oceanbase-ci";
    private static final String TEST_TENANT = "spark";
    private static final String SYS_PASSWORD = "123456";
    private static final String TEST_PASSWORD = "654321";

    public static final Network NETWORK = Network.newNetwork();

    public static final GenericContainer<?> FIX_CONTAINER =
            new FixedHostPortGenericContainer<>("oceanbase/oceanbase-ce:latest")
                    .withNetwork(NETWORK)
                    .withEnv("OB_TENANT_PASSWORD", TEST_PASSWORD)
                    .withEnv("MODE", "slim")
                    .withEnv("OB_CLUSTER_NAME", CLUSTER_NAME)
                    .withEnv("OB_SYS_PASSWORD", SYS_PASSWORD)
                    .withEnv("OB_DATAFILE_SIZE", "2G")
                    .withEnv("OB_LOG_DISK_SIZE", "4G")
                    .withStartupTimeout(Duration.ofMinutes(6))
                    .withFixedExposedPort(2881, 2881)
                    .withFixedExposedPort(2882, 2882)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    public void initialize(Connection conn, String sqlFile) {
        final URL file = getClass().getClassLoader().getResource(sqlFile);
        assertNotNull("Cannot locate " + sqlFile, file);

        try (Connection connection = conn;
                Statement statement = connection.createStatement()) {
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(file.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }

    public void dropTables(Connection conn, String... tableNames) throws SQLException {
        try (Connection connection = conn;
                Statement statement = connection.createStatement()) {
            for (String tableName : tableNames) {
                statement.execute("DROP TABLE " + tableName);
            }
        }
    }

    public void dropDataBases(Connection conn, String... databaseNames) throws SQLException {
        try (Connection connection = conn;
                Statement statement = connection.createStatement()) {
            for (String tableName : databaseNames) {
                statement.execute("DROP DATABASE " + tableName);
            }
        }
    }

    public void crateDataBases(Connection conn, String... databaseNames) throws SQLException {
        try (Connection connection = conn;
                Statement statement = connection.createStatement()) {
            for (String tableName : databaseNames) {
                statement.execute("CREATE DATABASE IF NOT EXISTS " + tableName);
            }
        }
    }

    public List<String> queryTable(Connection conn, String tableName) throws SQLException {
        return queryTable(conn, tableName, Collections.singletonList("*"));
    }

    public List<String> queryTable(Connection conn, String tableName, List<String> fields)
            throws SQLException {
        return queryTable(conn, tableName, fields, this::getRowString);
    }

    public List<String> queryTable(
            Connection conn, String tableName, List<String> fields, RowConverter rowConverter)
            throws SQLException {
        String sql = String.format("SELECT %s FROM %s", String.join(", ", fields), tableName);
        List<String> result = new ArrayList<>();

        try (Connection connection = conn;
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            ResultSetMetaData metaData = rs.getMetaData();

            while (rs.next()) {
                result.add(rowConverter.convert(rs, metaData.getColumnCount()));
            }
        }
        return result;
    }

    protected String getRowString(ResultSet rs, int columnCount) throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnCount; i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(rs.getObject(i + 1));
        }
        return sb.toString();
    }

    @FunctionalInterface
    public interface RowConverter {
        String convert(ResultSet rs, int columnCount) throws SQLException;
    }
}
