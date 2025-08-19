/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.omt.doris;

import com.oceanbase.omt.base.OceanBaseMySQLTestBase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Stream;

public class DorisContainerTestBase extends OceanBaseMySQLTestBase {

    protected static final Logger LOG = LoggerFactory.getLogger(DorisContainerTestBase.class);

    protected static final int DEFAULT_PARALLELISM = 1;
    protected static final DorisContainer DORIS_CONTAINER = createDorisContainer();

    public static final int DEFAULT_STARTUP_TIMEOUT_SECONDS = 240;

    private static DorisContainer createDorisContainer() {
        return new DorisContainer();
    }

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        FIX_CONTAINER.waitingFor(
                new LogMessageWaitStrategy()
                        .withRegEx(".*boot success!.*")
                        .withTimes(1)
                        .withStartupTimeout(Duration.ofMinutes(6)));
        FIX_CONTAINER.start();

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
        FIX_CONTAINER.stop();
        DORIS_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    public static boolean checkBackendAvailability() {
        try {
            Container.ExecResult rs =
                    DORIS_CONTAINER.execInContainer(
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

    public static void createDatabase(String databaseName) {
        try {
            Container.ExecResult rs =
                    DORIS_CONTAINER.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-uroot",
                            "-P9030",
                            "-h127.0.0.1",
                            String.format("-e CREATE DATABASE IF NOT EXISTS `%s`;", databaseName));

            if (rs.getExitCode() != 0) {
                throw new RuntimeException("Failed to create database." + rs.getStderr());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create database.", e);
        }
    }

    public static void createTable(
            String databaseName, String tableName, String primaryKey, List<String> schema) {
        try {
            Container.ExecResult rs =
                    DORIS_CONTAINER.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-uroot",
                            "-P9030",
                            "-h127.0.0.1",
                            String.format(
                                    "-e CREATE TABLE `%s`.`%s` (%s) UNIQUE KEY (`%s`) DISTRIBUTED BY HASH(`%s`) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\");",
                                    databaseName,
                                    tableName,
                                    String.join(", ", schema),
                                    primaryKey,
                                    primaryKey));

            if (rs.getExitCode() != 0) {
                throw new RuntimeException("Failed to create table." + rs.getStderr());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create table.", e);
        }
    }

    public static void dropDatabase(String databaseName) {
        try {
            Container.ExecResult rs =
                    DORIS_CONTAINER.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-uroot",
                            "-P9030",
                            "-h127.0.0.1",
                            String.format("-e DROP DATABASE IF EXISTS %s;", databaseName));

            if (rs.getExitCode() != 0) {
                throw new RuntimeException("Failed to drop database." + rs.getStderr());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to drop database.", e);
        }
    }

    public static void dropTable(String databaseName, String tableName) {
        try {
            Container.ExecResult rs =
                    DORIS_CONTAINER.execInContainer(
                            "mysql",
                            "--protocol=TCP",
                            "-uroot",
                            "-P9030",
                            "-h127.0.0.1",
                            String.format(
                                    "-e DROP TABLE IF EXISTS %s.%s;", databaseName, tableName));

            if (rs.getExitCode() != 0) {
                throw new RuntimeException("Failed to drop table." + rs.getStderr());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to drop table.", e);
        }
    }

    public static Connection getDorisConnection() {
        try {
            return DriverManager.getConnection(DORIS_CONTAINER.getJdbcUrl(), "root", "123456");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get connection.", e);
        }
    }
}
