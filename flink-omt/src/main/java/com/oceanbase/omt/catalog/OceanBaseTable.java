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

package com.oceanbase.omt.catalog;

import com.oceanbase.omt.partition.PartitionInfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class OceanBaseTable implements Serializable {
    private static final long serialVersionUID = 1L;
    private String database;
    private String table;
    private String tableComment;
    private List<OceanBaseColumn> fields;
    private List<String> keys = new ArrayList<>();
    private List<PartitionInfo> partitions = new ArrayList<>();
    private Map<String, String> properties = new HashMap<>();

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getTableComment() {
        return tableComment;
    }

    public List<OceanBaseColumn> getFields() {
        return fields;
    }

    public List<String> getKeys() {
        return keys;
    }

    public List<String> getFieldNames() {
        return fields.stream().map(OceanBaseColumn::getName).collect(Collectors.toList());
    }

    public List<PartitionInfo> getPartition() {
        return partitions;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public static final class TableSchemaBuilder {
        private String database;
        private String table;
        private String tableComment;
        private List<OceanBaseColumn> fields;
        private List<String> keys;
        private List<PartitionInfo> partitions;
        private Map<String, String> properties;

        private TableSchemaBuilder() {}

        public static TableSchemaBuilder aTableSchema() {
            return new TableSchemaBuilder();
        }

        public TableSchemaBuilder withDatabase(String database) {
            this.database = database;
            return this;
        }

        public TableSchemaBuilder withTable(String table) {
            this.table = table;
            return this;
        }

        public TableSchemaBuilder withTableComment(String tableComment) {
            this.tableComment = tableComment;
            return this;
        }

        public TableSchemaBuilder withFields(List<OceanBaseColumn> fields) {
            this.fields = fields;
            return this;
        }

        public TableSchemaBuilder withKeys(List<String> keys) {
            this.keys = keys;
            return this;
        }

        public TableSchemaBuilder withPartition(List<PartitionInfo> partitions) {
            this.partitions = partitions;
            return this;
        }

        public TableSchemaBuilder withProperties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public OceanBaseTable build() {
            OceanBaseTable tableSchema = new OceanBaseTable();
            tableSchema.database = this.database;
            tableSchema.partitions = this.partitions;
            tableSchema.properties = this.properties;
            tableSchema.tableComment = this.tableComment;
            tableSchema.table = this.table;
            tableSchema.fields = this.fields;
            tableSchema.keys = this.keys;
            return tableSchema;
        }
    }

    public TableIdentifier getTableIdentifier() {
        return new TableIdentifier(this.database, this.table);
    }

    public OceanBaseTable buildTableWithTableId(TableIdentifier identifier) {
        return OceanBaseTable.TableSchemaBuilder.aTableSchema()
                .withDatabase(identifier.getSchemaName())
                .withTable(identifier.getTableName())
                .withFields(this.getFields())
                .withKeys(this.getKeys())
                .withPartition(this.getPartition())
                .withProperties(this.getProperties())
                .withTableComment(this.getTableComment())
                .build();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        OceanBaseTable that = (OceanBaseTable) object;
        return Objects.equals(database, that.database)
                && Objects.equals(table, that.table)
                && Objects.equals(tableComment, that.tableComment)
                && Objects.equals(fields, that.fields)
                && Objects.equals(keys, that.keys)
                && Objects.equals(partitions, that.partitions)
                && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table, tableComment, fields, keys, partitions, properties);
    }

    @Override
    public String toString() {
        return "TableSchema{"
                + "database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + ", tableComment='"
                + tableComment
                + '\''
                + ", fields="
                + fields
                + ", keys="
                + keys
                + ", partitions="
                + partitions
                + ", properties="
                + properties
                + '}';
    }
}
