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

/** Doris配置接口，定义Doris数据源的配置参数 */
public interface DorisConfig {
    String JDBC_URL = "jdbc-url";
    /** FE节点的HTTP服务地址，用于通过Web服务器访问FE节点 */
    String FE_NODES = "fenodes";

    /** FE节点的JDBC连接地址，用于访问FE节点上的MySQL客户端 */
    String TABLE_IDENTIFIER = "table.identifier";

    /** 用于访问Doris集群的用户名 */
    String USERNAME = "username";

    /** 用于访问Doris集群的用户密码 */
    String PASSWORD = "password";
}
