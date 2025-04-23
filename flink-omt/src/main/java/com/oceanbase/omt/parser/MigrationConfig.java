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

import java.util.List;

public class MigrationConfig {
    private SourceMigrateConfig source;
    private OBMigrateConfig oceanbase;
    private List<Route> routes;
    private PipelineConfig pipeline;

    public SourceMigrateConfig getSource() {
        return source;
    }

    public void setSource(SourceMigrateConfig source) {
        this.source = source;
    }

    public OBMigrateConfig getOceanbase() {
        return oceanbase;
    }

    public void setOceanbase(OBMigrateConfig oceanbase) {
        this.oceanbase = oceanbase;
    }

    public List<Route> getRoutes() {
        return routes;
    }

    public void setRoutes(List<Route> routes) {
        this.routes = routes;
    }

    public PipelineConfig getPipeline() {
        return pipeline;
    }

    public void setPipeline(PipelineConfig pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    public String toString() {
        return "MigrationConfig{"
                + "source="
                + source
                + ", oceanbase="
                + oceanbase
                + ", rules="
                + routes
                + ", pipeline="
                + pipeline
                + '}';
    }
}
