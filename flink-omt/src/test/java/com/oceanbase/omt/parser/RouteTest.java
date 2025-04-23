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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class RouteTest {

    @Test
    public void testParseRoute() throws IOException {
        MigrationConfig migrationConfig = YamlParser.parseResource("yaml/config.yaml");
        List<Route> routes = migrationConfig.getRoutes();
        Assert.assertEquals(2, routes.size());

        // route1
        Route route1 = routes.get(0);
        Assert.assertEquals("test1.orders1", route1.getSourceTable());
        Assert.assertEquals("test1.order1", route1.getSinkTable());
        Assert.assertEquals("sync orders table to order", route1.getDescription());

        // route2
        Route route2 = routes.get(1);
        Assert.assertEquals("test1.orders[1-2]", route2.getSourceTable());
        Assert.assertEquals("route.order", route2.getSinkTable());
        Assert.assertEquals("sync orders table to route", route2.getDescription());
    }
}
