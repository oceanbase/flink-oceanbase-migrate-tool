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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class OBMigrateConfig {
    public static final String OB_DIRECT_LOAD_USERNAME = "direct-load.username";

    private String type = "jdbc";
    private String url;
    private String username;
    private String password;

    @JsonProperty("schema-name")
    private String schemaName;

    private String columnStoreType;

    private final Map<String, String> other = new HashMap<>();

    public String getColumnStoreType() {
        return columnStoreType;
    }

    public void setColumnStoreType(String columnStoreType) {
        this.columnStoreType = columnStoreType;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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
        return "OBMigrateConfig{"
                + "type='"
                + type
                + '\''
                + ", url='"
                + url
                + '\''
                + ", username='"
                + username
                + '\''
                + ", password='"
                + password
                + '\''
                + ", database='"
                + schemaName
                + '\''
                + ", other="
                + other
                + '}';
    }
}
