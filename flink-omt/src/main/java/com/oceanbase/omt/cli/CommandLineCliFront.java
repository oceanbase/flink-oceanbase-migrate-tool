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
package com.oceanbase.omt.cli;

import com.oceanbase.omt.DatabaseSyncBase;
import com.oceanbase.omt.DatabaseSyncConfig;
import com.oceanbase.omt.catalog.OceanBaseTable;
import com.oceanbase.omt.parser.MigrationConfig;
import com.oceanbase.omt.parser.PipelineConfig;
import com.oceanbase.omt.parser.YamlParser;
import com.oceanbase.omt.source.clickhouse.ClickHouseDatabaseSync;
import com.oceanbase.omt.source.doris.DorisDatabaseSync;
import com.oceanbase.omt.source.starrocks.StarRocksDatabaseSync;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;

public class CommandLineCliFront {
    private static final String SKIP_CONFIRM = "skip-confirm";
    private static StreamExecutionEnvironment flinkEnvironmentForTesting;
    private static JobClient jobClient;

    public static void main(String[] args) throws Exception {
        System.out.println("Input args: " + Arrays.asList(args) + ".\n");
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        Preconditions.checkState(params.has(DatabaseSyncConfig.CONFIG_FILE));
        System.out.println("load config: " + params.get(DatabaseSyncConfig.CONFIG_FILE));
        MigrationConfig migrationConfig =
                YamlParser.parse(params.get(DatabaseSyncConfig.CONFIG_FILE));
        boolean skipConfirm = params.has(SKIP_CONFIRM);
        String type = migrationConfig.getSource().getType();
        switch (type.toLowerCase()) {
            case DatabaseSyncConfig.STARROCKS_TYPE:
                createStarRocksSyncDatabase(migrationConfig, skipConfirm);
                break;

            case DatabaseSyncConfig.CLICKHOUSE_TYPE:
                createClickHouseSyncDatabase(migrationConfig, skipConfirm);
                break;

            case DatabaseSyncConfig.DORIS_TYPE:
                createDorisSyncDatabase(migrationConfig, skipConfirm);
                break;

            default:
                System.out.println("Unknown source type " + type);
                System.exit(1);
        }
    }

    private static void createStarRocksSyncDatabase(
            MigrationConfig migrationConfig, boolean skipConfirm) throws Exception {
        StarRocksDatabaseSync starRocksDatabaseSync = new StarRocksDatabaseSync(migrationConfig);
        CheckInfoPrinter(migrationConfig, starRocksDatabaseSync);
        starRocksDatabaseSync.createTableInOb();

        if (skipConfirm) {
            System.out.println("Skipping confirmation, starting data migration directly.");
            submitFlinkJob(starRocksDatabaseSync, migrationConfig);
        } else {
            System.out.println(
                    "The above table has been created in OceanBase."
                            + "Please check whether it meets your expectations. \n"
                            + "If it does, enter Y and press Enter to start data migration."
                            + "Otherwise, enter N and the tool will exit.(Y/N)\n");
            Scanner scanner = new Scanner(System.in);
            String nextLine = scanner.nextLine();
            if ("Y".equalsIgnoreCase(nextLine)) {
                System.out.println("Starting data migration.");
                submitFlinkJob(starRocksDatabaseSync, migrationConfig);
            } else if ("N".equalsIgnoreCase(nextLine)) {
                System.out.println("Flink-OMT exited!");
                System.exit(0);
            } else {
                System.exit(0);
            }
        }
    }

    private static void createClickHouseSyncDatabase(
            MigrationConfig migrationConfig, boolean skipConfirm) throws Exception {
        ClickHouseDatabaseSync clickHouseDatabaseSync = new ClickHouseDatabaseSync(migrationConfig);
        CheckInfoPrinter(migrationConfig, clickHouseDatabaseSync);
        clickHouseDatabaseSync.createTableInOb();

        if (skipConfirm) {
            System.out.println("Skipping confirmation, starting data migration directly.");
            submitFlinkJob(clickHouseDatabaseSync, migrationConfig);
        } else {
            System.out.println(
                    "The above table has been created in OceanBase."
                            + "Please check whether it meets your expectations. \n"
                            + "If it does, enter Y and press Enter to start data migration."
                            + "Otherwise, enter N and the tool will exit.(Y/N)\n");
            Scanner scanner = new Scanner(System.in);
            String nextLine = scanner.nextLine();
            if ("Y".equalsIgnoreCase(nextLine)) {
                System.out.println("Starting data migration.");
                submitFlinkJob(clickHouseDatabaseSync, migrationConfig);
            } else if ("N".equalsIgnoreCase(nextLine)) {
                System.out.println("Flink-OMT exited!");
                System.exit(0);
            } else {
                System.exit(0);
            }
        }
    }

    private static void createDorisSyncDatabase(
            MigrationConfig migrationConfig, boolean skipConfirm) throws Exception {
        DorisDatabaseSync dorisDatabaseSync = new DorisDatabaseSync(migrationConfig);
        try {
            CheckInfoPrinter(migrationConfig, dorisDatabaseSync);
            dorisDatabaseSync.createTableInOb();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create tables in OceanBase", e);
        }

        if (skipConfirm) {
            System.out.println("Skipping confirmation, starting data migration directly.");
            submitFlinkJob(dorisDatabaseSync, migrationConfig);
        } else {
            System.out.println(
                    "The above table has been created in OceanBase."
                            + "Please check whether it meets your expectations. \n"
                            + "If it does, enter Y and press Enter to start data migration."
                            + "Otherwise, enter N and the tool will exit.(Y/N)\n");
            Scanner scanner = new Scanner(System.in);
            String nextLine = scanner.nextLine();
            if ("Y".equalsIgnoreCase(nextLine)) {
                System.out.println("Starting data migration.");
                submitFlinkJob(dorisDatabaseSync, migrationConfig);
            } else if ("N".equalsIgnoreCase(nextLine)) {
                System.out.println("Flink-OMT exited!");
                System.exit(0);
            } else {
                System.exit(0);
            }
        }
    }

    private static void CheckInfoPrinter(
            MigrationConfig migrationConfig, DatabaseSyncBase databaseSyncBase) throws Exception {
        List<OceanBaseTable> obTables = databaseSyncBase.getObTables();
        if (CollectionUtil.isNullOrEmpty(obTables)) {
            String format =
                    "\n ============= Warning: No tables were found with [(%s)] to migrate, the job will exit. ============";
            System.err.println(String.format(format, migrationConfig.getSource().getTables()));
            System.exit(1);
        } else {
            String welInfo =
                    String.format(
                            "============= The following tables will be migrate from %s to oceanbase ============",
                            migrationConfig.getSource().getType());
            System.out.println(welInfo);
            obTables.forEach(
                    oceanBaseTable -> {
                        String table =
                                String.format(
                                        "%s.%s",
                                        oceanBaseTable.getDatabase(), oceanBaseTable.getTable());
                        System.out.println(table);
                    });
            System.out.println();
        }
    }

    private static void submitFlinkJob(
            DatabaseSyncBase databaseSyncBase, MigrationConfig migrationConfig) throws Exception {
        StreamExecutionEnvironment env =
                Objects.nonNull(flinkEnvironmentForTesting)
                        ? flinkEnvironmentForTesting
                        : StreamExecutionEnvironment.getExecutionEnvironment();

        // Allow pipeline config can be null
        PipelineConfig pipeline = migrationConfig.getPipeline();
        String jobName = null;
        if (Objects.nonNull(pipeline)) {
            Integer parallelism = pipeline.getParallelism();
            if (Objects.nonNull(parallelism)) {
                env.setParallelism(parallelism);
            }
            jobName = pipeline.getName();
        }

        databaseSyncBase.buildPipeline(env);
        if (StringUtils.isNullOrWhitespaceOnly(jobName)) {
            jobName =
                    String.format(
                            "Flink-OceanBase-Migrate-Tools-Sync-Database-%s",
                            migrationConfig.getSource().getType());
        }
        if (Objects.nonNull(flinkEnvironmentForTesting)) {
            jobClient = env.executeAsync();
        } else {
            env.execute(jobName);
        }
    }

    @VisibleForTesting
    public static JobClient getJobClient() {
        return jobClient;
    }

    // Only for testing, please do not use it in actual environment
    @VisibleForTesting
    public static void setStreamExecutionEnvironmentForTesting(
            StreamExecutionEnvironment environment) {
        flinkEnvironmentForTesting = environment;
    }
}
