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
package com.oceanbase.omt.source.doris;

import com.oceanbase.omt.partition.PartitionInfo;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DorisPartitionUtils {
    private static final String RANGE = "RANGE";
    private static final String LIST = "LIST";
    private static final String ALL_KEY = "ALL KEY";
    private static final String HASH = "HASH";
    private static final String KEY = "KEY";

    public static class RangeInfo {
        private final String type;
        private final String key;

        public RangeInfo(String type, String key) {
            this.type = type;
            this.key = key;
        }

        public String getType() {
            return type;
        }

        public String getKey() {
            return key;
        }

        @Override
        public String toString() {
            return "Type: " + type + ", Key: " + key;
        }
    }

    // range supports String DATE BIGINT
    public static List<RangeInfo> extractPartitionInfo(
            String input, List<String> partitionKeyTypes) {
        List<RangeInfo> rangeInfos = new ArrayList<>();

        String regex = "types:\\s*\\[(.*?)]\\s*;\\s*keys:\\s*\\[(.*?)]";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);
        // if multiple partition fields
        while (matcher.find()) {
            String type = matcher.group(1).trim();
            StringBuilder comKey = getComKey(matcher, type, partitionKeyTypes);
            rangeInfos.add(new RangeInfo(type, comKey.toString()));
        }

        return rangeInfos;
    }

    private static StringBuilder getComKey(
            Matcher matcher, String type, List<String> partitionKeyTypes) {
        String key = matcher.group(2).trim();
        String[] types = type.split(",");
        String[] keys = key.split(",");
        StringBuilder comKey = new StringBuilder();
        for (int i = 0; i < types.length; i++) {
            String resultKey = getRangeValue(types[i].trim(), keys[i]);
            comKey.append(resultKey.trim());
            if (i != types.length - 1) {
                comKey.append(",");
            }
        }
        return comKey;
    }

    public static Tuple2<String, String> getRange(List<RangeInfo> rangeInfos) {
        if (rangeInfos.size() != 2) {
            return null;
        }
        return Tuple2.of(rangeInfos.get(0).getKey(), rangeInfos.get(1).getKey());
    }

    public static String buildOBPartitionWithDDL(String ddl, List<PartitionInfo> partitions) {
        String partitionSqlTemplate = "PARTITION BY %s COLUMNS(%s) %s (%s)";
        String bucketSqlTemplate = "SUBPARTITION BY KEY(%s)";
        String partitionRanges = "PARTITION %s VALUES LESS THAN(%s)";
        String partitionLists = "PARTITION %s VALUES IN(%s)";
        String partitionHash = "PARTITION BY %s(%s) PARTITIONS %s";

        if (partitions.isEmpty()) {
            return ddl;
        }

        String distributionKey = partitions.get(0).getDistributionKey();
        Integer buckets = partitions.get(0).getBuckets();
        String partitionKey = partitions.get(0).getPartitionKey();
        boolean rangeFlag = StringUtils.isNotBlank(partitions.get(0).getRange());
        boolean listFlag = StringUtils.isNotBlank(partitions.get(0).getList());

        List<String> extents = new ArrayList<>();
        if (rangeFlag) {
            for (int i = 0; i < partitions.size(); i++) {
                List<RangeInfo> rangeInfos =
                        extractPartitionInfo(
                                partitions.get(i).getRange(),
                                partitions.get(i).getPartitionKeyType());
                Tuple2<String, String> rangeInfo = getRange(rangeInfos);
                if (rangeInfo != null) {
                    extents.add(
                            String.format(
                                    partitionRanges,
                                    partitions.get(i).getPartitionName(),
                                    rangeInfo.f1));
                }
            }
        }

        if (listFlag) {
            for (PartitionInfo partition : partitions) {
                String listPartition =
                        toListPartition(
                                partitionLists,
                                partition.getList(),
                                partition.getPartitionKeyType(),
                                partition.getPartitionName());
                extents.add(listPartition);
            }
        }

        String partitionType = rangeFlag ? RANGE : listFlag ? LIST : null;
        String bucketSql =
                distributionKey != null && !distributionKey.equalsIgnoreCase(ALL_KEY)
                        ? String.format(bucketSqlTemplate, distributionKey)
                        : null;

        String partitionSql =
                partitionType != null
                        ? String.format(
                                partitionSqlTemplate,
                                partitionType,
                                partitionKey,
                                bucketSql,
                                extents.stream().distinct().collect(Collectors.joining(",")))
                        : null;
        if (partitionType == null
                && distributionKey != null
                && !distributionKey.equalsIgnoreCase(ALL_KEY)) {
            String partitionHashType = distributionKey.split(",").length == 1 ? HASH : KEY;
            partitionSql =
                    String.format(partitionHash, partitionHashType, distributionKey, buckets);
        }
        return partitionSql != null ? ddl + partitionSql : ddl;
    }

    // list support specialTypes
    public static String toListPartition(
            String listPartitionTemplate,
            String input,
            List<String> partitionKeyTypes,
            String partitionName) {
        input = input.substring(1, input.length() - 1);
        String[] pairs = input.split(" , ");
        List<String> partitionInfos = new ArrayList<>();
        // Process each group
        for (String pair : pairs) {
            pair = pair.split(";")[1];
            pair = pair.substring(8, pair.length() - 1);
            String[] split = pair.split(", ");
            StringBuilder resultValue = new StringBuilder();
            for (int i = 0; i < split.length; i++) {
                String listValue =
                        getListValue(
                                partitionKeyTypes.size() == 1
                                        ? partitionKeyTypes.get(0)
                                        : partitionKeyTypes.get(i),
                                split[i].trim());
                resultValue.append(listValue);
                if (i != split.length - 1) {
                    resultValue.append(",");
                }
            }
            partitionInfos.add(String.format(listPartitionTemplate, partitionName, resultValue));
        }
        return String.join(",", partitionInfos);
    }

    public static String getListValue(String partitionKeyTypes, String listValue) {
        switch (partitionKeyTypes.toUpperCase()) {
            case "INT":
            case "TINYINT":
            case "SMALLINT":
            case "BIGINT":
            case "FLOAT":
            case "DOUBLE":
            case "BOOLEAN":
                return listValue;
            default:
                return Arrays.stream(listValue.split(","))
                        .map(s -> String.format("'%s'", s))
                        .collect(Collectors.joining(","));
        }
    }

    public static String getRangeValue(String type, String listValue) {
        switch (type.toUpperCase()) {
            case "INT":
            case "TINYINT":
            case "SMALLINT":
            case "BIGINT":
            case "FLOAT":
            case "DOUBLE":
            case "BOOLEAN":
                return listValue;
            default:
                return String.format("'%s'", listValue);
        }
    }
}
