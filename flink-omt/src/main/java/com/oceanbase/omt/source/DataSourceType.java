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
package com.oceanbase.omt.source;

import lombok.Getter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@Getter
public enum DataSourceType {
    STAR_ROCKS("starrocks"),
    CLICK_HOUSE("clickhouse"),
    DORIS("doris");

    private String value;

    DataSourceType(String value) {
        this.value = value;
    }

    private static final Map<String, DataSourceType> map = new HashMap<>();

    public static final Function<String, DataSourceType> FROM_VALUE =
            v -> {
                return Arrays.stream(values())
                        .filter(t -> t.getValue().equalsIgnoreCase(v))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("Unknown value: " + v));
            };
}
