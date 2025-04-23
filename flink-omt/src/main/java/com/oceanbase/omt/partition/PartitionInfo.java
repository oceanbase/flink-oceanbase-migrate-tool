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

import java.io.Serializable;
import java.util.List;

public class PartitionInfo implements Serializable {

    private String partitionId;

    private String partitionName;

    private String partitionKey;

    private String distributionKey;

    private Integer buckets;

    private String range;

    private String list;

    private List<String> partitionKeyType;

    public String getPartitionId() {
        return partitionId;
    }

    public PartitionInfo withPartitionId(String partitionId) {
        this.partitionId = partitionId;
        return this;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public PartitionInfo withPartitionName(String partitionName) {
        this.partitionName = partitionName;
        return this;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public PartitionInfo withPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
        return this;
    }

    public String getDistributionKey() {
        return distributionKey;
    }

    public PartitionInfo withDistributionKey(String distributionKey) {
        this.distributionKey = distributionKey;
        return this;
    }

    public String getRange() {
        return range;
    }

    public PartitionInfo withRange(String range) {
        this.range = range;
        return this;
    }

    public String getList() {
        return list;
    }

    public PartitionInfo withList(String list) {
        this.list = list;
        return this;
    }

    public Integer getBuckets() {
        return buckets;
    }

    public PartitionInfo withBuckets(Integer buckets) {
        this.buckets = buckets;
        return this;
    }

    public List<String> getPartitionKeyType() {
        return partitionKeyType;
    }

    public PartitionInfo withPartitionKeyType(List<String> partitionKeyType) {
        this.partitionKeyType = partitionKeyType;
        return this;
    }

    @Override
    public String toString() {
        return "PartitionInfo{"
                + "partitionId='"
                + partitionId
                + '\''
                + ", partitionName='"
                + partitionName
                + '\''
                + ", partitionKey='"
                + partitionKey
                + '\''
                + ", distributionKey='"
                + distributionKey
                + '\''
                + ", range='"
                + range
                + '\''
                + ", list='"
                + list
                + '\''
                + '}';
    }
}
