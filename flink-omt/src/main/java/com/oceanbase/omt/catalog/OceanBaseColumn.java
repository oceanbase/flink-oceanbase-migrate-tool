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

import java.io.Serializable;
import java.util.Objects;

public class OceanBaseColumn implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private int ordinalPosition;
    private String typeString;
    private String defaultValue;
    private Integer columnSize;
    private Integer numericScale;
    private String comment;
    private Boolean nullable;
    private String columnType;
    private String dataType;

    public String getName() {
        return name;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public String getTypeString() {
        return typeString;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setTypeString(String typeString) {
        this.typeString = typeString;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public Integer getColumnSize() {
        return columnSize;
    }

    public Integer getNumericScale() {
        return numericScale;
    }

    public String getComment() {
        return comment;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public String getDataType() {
        return dataType;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (!(object instanceof OceanBaseColumn)) return false;
        OceanBaseColumn that = (OceanBaseColumn) object;
        return ordinalPosition == that.ordinalPosition
                && Objects.equals(name, that.name)
                && Objects.equals(typeString, that.typeString)
                && Objects.equals(defaultValue, that.defaultValue)
                && Objects.equals(columnSize, that.columnSize)
                && Objects.equals(numericScale, that.numericScale)
                && Objects.equals(comment, that.comment)
                && Objects.equals(nullable, that.nullable)
                && Objects.equals(dataType, that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                name,
                ordinalPosition,
                typeString,
                defaultValue,
                columnSize,
                numericScale,
                comment,
                nullable,
                dataType);
    }

    @Override
    public String toString() {
        return "FieldSchema{"
                + "name='"
                + name
                + '\''
                + ", ordinalPosition="
                + ordinalPosition
                + ", typeString='"
                + typeString
                + '\''
                + ", defaultValue='"
                + defaultValue
                + '\''
                + ", columnSize='"
                + columnSize
                + '\''
                + ", numericScale="
                + numericScale
                + ", comment='"
                + comment
                + '\''
                + ", nullable="
                + nullable
                + ", columnType="
                + columnType
                + ", dataType="
                + dataType
                + '}';
    }

    public static final class FieldSchemaBuilder {
        private String name;
        private int ordinalPosition;
        private String typeString;
        private String defaultValue;
        private Integer columnSize;
        private Integer numericScale;
        private String comment;
        private Boolean nullable;
        private String columnType;
        private String dataType;

        private FieldSchemaBuilder() {}

        public static FieldSchemaBuilder aFieldSchema() {
            return new FieldSchemaBuilder();
        }

        public FieldSchemaBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public FieldSchemaBuilder withOrdinalPosition(int ordinalPosition) {
            this.ordinalPosition = ordinalPosition;
            return this;
        }

        public FieldSchemaBuilder withTypeString(String typeString) {
            this.typeString = typeString;
            return this;
        }

        public FieldSchemaBuilder withDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public FieldSchemaBuilder withColumnSize(Integer columnSize) {
            this.columnSize = columnSize;
            return this;
        }

        public FieldSchemaBuilder withDataType(String dataType) {
            this.dataType = dataType;
            return this;
        }

        public FieldSchemaBuilder withNumericScale(Integer numericScale) {
            this.numericScale = numericScale;
            return this;
        }

        public FieldSchemaBuilder withComment(String comment) {
            this.comment = comment;
            return this;
        }

        public FieldSchemaBuilder withNullable(Boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public FieldSchemaBuilder withColumnType(String columnType) {
            this.columnType = columnType;
            return this;
        }

        public OceanBaseColumn build() {
            OceanBaseColumn fieldSchema = new OceanBaseColumn();
            fieldSchema.typeString = this.typeString;
            fieldSchema.name = this.name;
            fieldSchema.numericScale = this.numericScale;
            fieldSchema.nullable = this.nullable;
            fieldSchema.columnSize = this.columnSize;
            fieldSchema.defaultValue = this.defaultValue;
            fieldSchema.ordinalPosition = this.ordinalPosition;
            fieldSchema.comment = this.comment;
            fieldSchema.columnType = this.columnType;
            fieldSchema.dataType = this.dataType;
            return fieldSchema;
        }
    }
}
