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
package com.oceanbase.omt.utils;

import com.oceanbase.omt.parser.OBMigrateConfig;
import com.oceanbase.omt.parser.SourceMigrateConfig;
import com.oceanbase.omt.source.starrocks.StarRocksConfig;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

import java.util.Map;

public class DataSourceUtils {
    private static volatile HikariDataSource dataSource;
    private static volatile HikariDataSource sourceSource;

    private DataSourceUtils() {}

    public static DataSource getOBDataSource(OBMigrateConfig obMigrateConfig) {
        if (dataSource == null) {
            synchronized (DataSourceUtils.class) {
                if (dataSource == null) {
                    HikariConfig config = new HikariConfig();
                    config.setJdbcUrl(obMigrateConfig.getUrl());
                    config.setUsername(obMigrateConfig.getUsername());
                    config.setPassword(obMigrateConfig.getPassword());
                    config.setMaximumPoolSize(10);

                    dataSource = new HikariDataSource(config);
                }
            }
        }
        return dataSource;
    }

    public static DataSource getSourceDataSource(SourceMigrateConfig sourceMigrateConfig) {
        Map<String, String> other = sourceMigrateConfig.getOther();
        if (sourceSource == null) {
            synchronized (DataSourceUtils.class) {
                if (sourceSource == null) {
                    HikariConfig config = new HikariConfig();
                    config.setJdbcUrl(other.get(StarRocksConfig.JDBC_URL));
                    config.setUsername(other.get(StarRocksConfig.USERNAME));
                    config.setPassword(other.get(StarRocksConfig.PASSWORD));
                    config.setMaximumPoolSize(10);

                    sourceSource = new HikariDataSource(config);
                }
            }
        }
        return sourceSource;
    }
}
