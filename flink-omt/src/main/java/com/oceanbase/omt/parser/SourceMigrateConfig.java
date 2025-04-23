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
package com.oceanbase.omt.parser;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.HashMap;
import java.util.Map;

public class SourceMigrateConfig {
    private String type;
    private String database;
    private String tables;
    private final Map<String, String> other = new HashMap<>();

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTables() {
        return tables;
    }

    public void setTables(String tables) {
        this.tables = tables;
    }

    public Map<String, String> getOther() {
        return other;
    }

    @JsonAnySetter
    public void setOther(String key, String value) {
        this.other.put(key, value);
    }

    @Override
    public String toString() {
        return "SourceMigrateConfig{"
                + "type='"
                + type
                + '\''
                + ", database='"
                + database
                + '\''
                + ", table='"
                + tables
                + '\''
                + ", other="
                + other
                + '}';
    }
}
