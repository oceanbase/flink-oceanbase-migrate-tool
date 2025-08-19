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

import com.oceanbase.omt.source.clickhouse.ClickHouseTypeConverter;
import com.oceanbase.omt.source.doris.DorisTypeConverter;
import com.oceanbase.omt.source.starrocks.StarRocksTypeConverter;

import java.util.HashMap;
import java.util.Map;

public class TypeConverterFactory {
    private static final Map<DataSourceType, TypeConverter> converters = new HashMap<>();

    static {
        converters.put(DataSourceType.STAR_ROCKS, new StarRocksTypeConverter());
        converters.put(DataSourceType.CLICK_HOUSE, new ClickHouseTypeConverter());
        converters.put(DataSourceType.DORIS, new DorisTypeConverter());
    }

    public static TypeConverter getConverter(DataSourceType type) {
        if (type == null) {
            throw new IllegalArgumentException("DataSourceType cannot be null");
        }
        return converters.get(type);
    }
}
