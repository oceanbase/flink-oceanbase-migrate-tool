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

import org.apache.flink.shaded.guava31.com.google.common.io.Resources;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;

public class YamlParser {

    public static MigrationConfig parse(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        File file = Paths.get(path).toFile();
        return mapper.readValue(file, MigrationConfig.class);
    }

    public static MigrationConfig parseResource(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(Resources.getResource(path).openStream(), MigrationConfig.class);
    }

    /** 从字符串解析 YAML 配置。 */
    public static MigrationConfig parseFromString(String yamlContent) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new StringReader(yamlContent), MigrationConfig.class);
    }
}
