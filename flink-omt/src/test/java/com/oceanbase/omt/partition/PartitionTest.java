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
package com.oceanbase.omt.partition;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class PartitionTest {
    private List<PartitionInfo> createMultiListPartitionInfos() {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        PartitionInfo partitionInfo1 = new PartitionInfo();
        partitionInfo1
                .withPartitionId("33952")
                .withBuckets(2)
                .withPartitionName("p202204_California")
                .withDistributionKey("id")
                .withPartitionKey("dt, city")
                .withPartitionKeyType(Lists.newArrayList("STRING", "STRING"))
                .withList(
                        "(('2022-04-01', 'Los Angeles'), "
                                + "('2022-04-01', 'San Francisco'), "
                                + "('2022-04-02', 'Los Angeles'), "
                                + "('2022-04-02', 'San Francisco'), "
                                + "('2022-04-01', 'Houston'), "
                                + "('2022-04-01', 'Dallas'), "
                                + "('2022-04-02', 'Houston'), "
                                + "('2022-04-02', 'Dallas'))");
        partitionInfos.add(partitionInfo1);
        return partitionInfos;
    }

    private List<PartitionInfo> createOneListPartitionInfos() {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        PartitionInfo partitionInfo1 = new PartitionInfo();
        partitionInfo1
                .withPartitionId("33952")
                .withBuckets(2)
                .withPartitionName("p202204_California")
                .withDistributionKey("string_col,date_col")
                .withPartitionKey("int_col,char_col")
                .withPartitionKeyType(Lists.newArrayList("STRING", "STRING"))
                .withList("(('200'))");
        PartitionInfo partitionInfo2 = new PartitionInfo();
        partitionInfo2
                .withPartitionId("33953")
                .withBuckets(2)
                .withPartitionName("p202204_California")
                .withDistributionKey("string_col,date_col")
                .withPartitionKey("int_col,char_col")
                .withPartitionKeyType(Lists.newArrayList("STRING", "STRING"))
                .withList("(('-32768'))");
        partitionInfos.add(partitionInfo1);
        partitionInfos.add(partitionInfo2);
        return partitionInfos;
    }

    private List<PartitionInfo> crateSpecialTypesPartitionFieldsInfos() {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        PartitionInfo partitionInfo1 = new PartitionInfo();
        partitionInfo1
                .withPartitionId("33987")
                .withBuckets(2)
                .withPartitionName("p20251209")
                .withDistributionKey("string_col, date_col")
                .withPartitionKey("boolean_col, char_col")
                .withPartitionKeyType(Lists.newArrayList("BOOLEAN", "STRING"))
                .withList("(('TRUE', 'char1'))");
        PartitionInfo partitionInfo2 = new PartitionInfo();
        partitionInfo2
                .withPartitionId("33997")
                .withBuckets(2)
                .withPartitionName("p20251210")
                .withDistributionKey("string_col, date_col")
                .withPartitionKey("boolean_col, char_col")
                .withPartitionKeyType(Lists.newArrayList("BOOLEAN", "STRING"))
                .withList("(('FALSE', 'char2'))");
        partitionInfos.add(partitionInfo1);
        partitionInfos.add(partitionInfo2);
        return partitionInfos;
    }

    private List<PartitionInfo> createRangePartitionInfos() {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        PartitionInfo partitionInfo1 = new PartitionInfo();
        partitionInfo1
                .withPartitionId("33987")
                .withBuckets(2)
                .withPartitionName("p20251209")
                .withDistributionKey("event_day, site_id")
                .withPartitionKey("event_day")
                .withRange(
                        "[types: [DATETIME]; keys: [2025-12-09 00:00:00]; ..types: [DATETIME]; keys: [2025-12-10 00:00:00]; )");
        PartitionInfo partitionInfo2 = new PartitionInfo();
        partitionInfo2
                .withPartitionId("33997")
                .withBuckets(2)
                .withPartitionName("p20251210")
                .withDistributionKey("event_day, site_id")
                .withPartitionKey("event_day")
                .withRange(
                        "[types: [DATETIME]; keys: [2025-12-09 00:00:00]; ..types: [DATETIME]; keys: [2025-12-10 00:00:00]; )");
        partitionInfos.add(partitionInfo1);
        partitionInfos.add(partitionInfo2);
        return partitionInfos;
    }

    private List<PartitionInfo> crateMultipleRangPartitionFieldsInfos() {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        PartitionInfo partitionInfo1 = new PartitionInfo();
        partitionInfo1
                .withPartitionId("33987")
                .withBuckets(2)
                .withPartitionName("p20251209")
                .withDistributionKey("int_col")
                .withPartitionKey("date_col, bigint_col")
                .withRange(
                        "[types: [DATE, BIGINT]; keys: [2023-01-02, 1000000001]; ..types: [DATE, BIGINT]; keys: [2023-02-02, 2000000000]; )");
        PartitionInfo partitionInfo2 = new PartitionInfo();
        partitionInfo2
                .withPartitionId("33997")
                .withBuckets(2)
                .withPartitionName("p20251210")
                .withDistributionKey("int_col")
                .withPartitionKey("date_col, bigint_col")
                .withRange(
                        "[types: [DATE, BIGINT]; keys: [2022-01-02, 0]; ..types: [DATE, BIGINT]; keys: [2023-01-02, 1000000001]; )");
        partitionInfos.add(partitionInfo1);
        partitionInfos.add(partitionInfo2);
        return partitionInfos;
    }

    private List<PartitionInfo> createSinglePartitionInfo(String distributionKey) {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        PartitionInfo partitionInfo = new PartitionInfo();
        partitionInfo.withBuckets(2).withDistributionKey(distributionKey);
        partitionInfos.add(partitionInfo);
        return partitionInfos;
    }

    @Test
    public void testMultiListPartitionDDL() {
        String expectedDDL =
                "PARTITION BY LIST COLUMNS(dt, city) "
                        + "SUBPARTITION BY KEY(id) "
                        + "(PARTITION p0 VALUES IN('2022-04-01','Los Angeles'),"
                        + "PARTITION p1 VALUES IN('2022-04-01','San Francisco'),"
                        + "PARTITION p2 VALUES IN('2022-04-02','Los Angeles'),"
                        + "PARTITION p3 VALUES IN('2022-04-02','San Francisco'),"
                        + "PARTITION p4 VALUES IN('2022-04-01','Houston'),"
                        + "PARTITION p5 VALUES IN('2022-04-01','Dallas'),"
                        + "PARTITION p6 VALUES IN('2022-04-02','Houston'),"
                        + "PARTITION p7 VALUES IN('2022-04-02','Dallas'))";
        List<PartitionInfo> partitionInfos = createMultiListPartitionInfos();
        String actualDDL = PartitionUtils.buildOBPartitionWithDDL("", partitionInfos);
        Assertions.assertEquals(expectedDDL, actualDDL);
    }

    @Test
    public void testSpecialTypesListPartitionDDL() {
        String expectedDDL =
                "PARTITION BY LIST COLUMNS(boolean_col, char_col) "
                        + "SUBPARTITION BY KEY(string_col, date_col) "
                        + "(PARTITION p0 VALUES IN(TRUE,'char1'),"
                        + "PARTITION p1 VALUES IN(FALSE,'char2'))";
        List<PartitionInfo> partitionInfos = crateSpecialTypesPartitionFieldsInfos();
        String actualDDL = PartitionUtils.buildOBPartitionWithDDL("", partitionInfos);
        Assertions.assertEquals(expectedDDL, actualDDL);
    }

    @Test
    public void testOneListPartitionDDL() {
        String expectedDDL =
                "PARTITION BY LIST COLUMNS(int_col,char_col) "
                        + "SUBPARTITION BY KEY(string_col,date_col) "
                        + "(PARTITION p0 VALUES IN('200'),"
                        + "PARTITION p1 VALUES IN('-32768'))";
        List<PartitionInfo> partitionInfos = createOneListPartitionInfos();
        String actualDDL = PartitionUtils.buildOBPartitionWithDDL("", partitionInfos);
        Assertions.assertEquals(expectedDDL, actualDDL);
    }

    @Test
    public void testRangePartitionDDL() {
        String expectedDDL =
                "PARTITION BY RANGE COLUMNS(event_day) "
                        + "SUBPARTITION BY KEY(event_day, site_id) "
                        + "(PARTITION p0 VALUES LESS THAN('2025-12-10 00:00:00'),"
                        + "PARTITION p1 VALUES LESS THAN('2025-12-10 00:00:00'))";

        List<PartitionInfo> partitionInfos = createRangePartitionInfos();
        String actualDDL = PartitionUtils.buildOBPartitionWithDDL("", partitionInfos);
        Assertions.assertEquals(expectedDDL, actualDDL);
    }

    @Test
    public void testMultipleRangPartitionFields() {
        String expectedDDL =
                "PARTITION BY RANGE COLUMNS(date_col, bigint_col) "
                        + "SUBPARTITION BY KEY(int_col) "
                        + "(PARTITION p0 VALUES LESS THAN('2023-02-02',2000000000),"
                        + "PARTITION p1 VALUES LESS THAN('2023-01-02',1000000001))";
        List<PartitionInfo> partitionInfos = crateMultipleRangPartitionFieldsInfos();
        String actualDDL = PartitionUtils.buildOBPartitionWithDDL("", partitionInfos);
        Assertions.assertEquals(expectedDDL, actualDDL);
    }

    @Test
    public void testHashPartitionDDL() {
        String expectedDDL = "PARTITION BY HASH(order_id) PARTITIONS 2";
        List<PartitionInfo> partitionInfos = createSinglePartitionInfo("order_id");
        String actualDDL = PartitionUtils.buildOBPartitionWithDDL("", partitionInfos);
        Assertions.assertEquals(expectedDDL, actualDDL);
    }

    @Test
    public void testKeyPartitionDDL() {
        String expectedDDL = "PARTITION BY KEY(order_id,user_id) PARTITIONS 2";
        List<PartitionInfo> partitionInfos = createSinglePartitionInfo("order_id,user_id");
        String actualDDL = PartitionUtils.buildOBPartitionWithDDL("", partitionInfos);
        Assertions.assertEquals(expectedDDL, actualDDL);
    }
}
