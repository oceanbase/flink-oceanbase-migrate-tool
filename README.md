# flink-oceanbase-migrate-tool

[English](README_EN.md) | 简体中文

# 项目介绍

flink-oceanbase-migrate-tool（简称 Flink-OMT）是 OceanBase 提供的数据迁移工具，用于将源数据库的数据通过 Flink 导入 OceanBase。该工具使得用户能够以 YAML 配置文件的形式，优雅地定义其数据同步流程，自动化生成定制化的 Flink 算子并且提交 Flink 作业。
Flink-OMT 深度集成并由 Apache Flink 驱动，提供以下核心功能：

+ ✅ 端到端的数据集成框架
+ ✅ 具备表结构自动同步的能力
+ ✅ 支持多库多表同步
+ ✅ [支持多库多表路由同步](./doc/route.md)

当前 Flink-OMT 仅支持StarRocks作为源数据库。

# 编译与构建

```shell
git clone https://github.com/oceanbase/flink-oceanbase-migrate-tool.git
cd flink-oceanbase-migrate-tool
mvn clean package -DskipTests

# Or specify the flink version to build
mvn clean package -DskipTests -Dflink.majorVersion=1.19 -Dflink.version=1.19.1
```

# 使用

```shell
<FLINK_HOME>bin/flink run \
     -D execution.checkpointing.interval=10s\
     -D parallelism.default=1\
     -c com.oceanbase.omt.cli.CommandLineCliFront\
     lib/flink-omt-1.0-SNAPSHOT.jar \
     -config config.yaml
```

# OceanBase Sink

OceanBase Sink支持以JDBC或者旁路导入的方式，同步数据到OceanBase。

+ OceanBase Sink的实现依赖于[flink-connector-oceanbase](https://github.com/oceanbase/flink-connector-oceanbase)项目。
+ 关于 OceanBase 的旁路导入功能，见 [旁路导入文档](https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000001428636)

## 示例

+ JDBC方式

```yaml
oceanbase:
  url: jdbc:mysql://localhost:2881/test
  username: root@test
  password: ****
  schema-name: test
```

+ 旁路导入方式

```yaml
oceanbase:
  type: direct-load
  url: jdbc:mysql://localhost:2881/test
  username: root@test
  host: localhost
  port: 2882
  password: ****
  schema-name: test
```

## OceanBase Sink配置项

### JDBC方式配置项

+ 实现依赖于[flink-connector-oceanbase](https://github.com/oceanbase/flink-connector-oceanbase)项目，可参考相关文档：[https://github.com/oceanbase/flink-connector-oceanbase/blob/main/docs/sink/flink-connector-oceanbase_cn.md](https://github.com/oceanbase/flink-connector-oceanbase/blob/main/docs/sink/flink-connector-oceanbase_cn.md)

|                 参数名                  |           默认值            |    类型    |                                    描述                                     |
|--------------------------------------|--------------------------|----------|---------------------------------------------------------------------------|
| type                                 | jdbc                     | String   | OceanBase Sink数据同步方式，支持jdbc和direct-load，默认为jdbc。                          |
| url                                  |                          | String   | 数据库的 JDBC url。                                                            |
| username                             |                          | String   | 连接用户名。                                                                    |
| password                             |                          | String   | 连接密码。                                                                     |
| schema-name                          |                          | String   | 连接的 schema 名或 db 名。                                                       |
| table-name                           |                          | String   | 表名。                                                                       |
| driver-class-name                    | com.mysql.cj.jdbc.Driver | String   | 驱动类名，默认为 'com.mysql.cj.jdbc.Driver'，如果设置了其他值，需要手动引入对应的依赖。                 |
| druid-properties                     |                          | String   | Druid 连接池属性，多个值用分号分隔。                                                     |
| sync-write                           | false                    | Boolean  | 是否开启同步写，设置为 true 时将不使用 buffer 直接写入数据库。                                    |
| buffer-flush.interval                | 1s                       | Duration | 缓冲区刷新周期。设置为 '0' 时将关闭定期刷新。                                                 |
| buffer-flush.buffer-size             | 1000                     | Integer  | 缓冲区大小。                                                                    |
| max-retries                          | 3                        | Integer  | 失败重试次数。                                                                   |
| memstore-check.enabled               | true                     | Boolean  | 是否开启内存检查。                                                                 |
| memstore-check.threshold             | 0.9                      | Double   | 内存使用的阈值相对最大限制值的比例。                                                        |
| memstore-check.interval              | 30s                      | Duration | 内存使用检查周期。                                                                 |
| partition.enabled                    | false                    | Boolean  | 是否启用分区计算功能，按照分区来写数据。仅当 'sync-write' 和 'direct-load.enabled' 都为 false 时生效。 |
| table.oracle-tenant-case-insensitive | true                     | Boolean  | 默认情况下，在 Oracle 租户下，Schema名和列名不区分大小写。                                      |

### 旁路导入配置项

+ 实现依赖于[flink-connector-oceanbase](https://github.com/oceanbase/flink-connector-oceanbase)项目旁路导入模块，可参考相关文档：[https://github.com/oceanbase/flink-connector-oceanbase/blob/main/docs/sink/flink-connector-oceanbase-directload_cn.md](https://github.com/oceanbase/flink-connector-oceanbase/blob/main/docs/sink/flink-connector-oceanbase-directload_cn.md)

|           参数名           |   默认值   |    类型    |                                                                                                                                        描述                                                                                                                                        |
|-------------------------|---------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type                    | jdbc    | String   | OceanBase Sink数据同步方式，支持jdbc和direct-load，默认为jdbc。                                                                                                                                                                                                                                 |
| url                     |         | String   | 数据库的 JDBC url。用户表结构同步，旁路导入数据同步本身，不依赖jdbc。                                                                                                                                                                                                                                        |
| username                |         |          | 连接用户名。                                                                                                                                                                                                                                                                           |
| password                |         | String   | 密码。                                                                                                                                                                                                                                                                              |
| host                    |         | String   | OceanBase数据库的host地址。                                                                                                                                                                                                                                                             |
| port                    |         | Integer  | 旁路导入使用的RPC端口。                                                                                                                                                                                                                                                                    |
| tenant-name             |         | String   | 租户名。                                                                                                                                                                                                                                                                             |
| schema-name             |         | String   | schema名或DB名。                                                                                                                                                                                                                                                                     |
| table-name              |         | String   | 表名。                                                                                                                                                                                                                                                                              |
| parallel                | 8       | Integer  | 旁路导入服务端的并发度。该参数决定了服务端使用多少cpu资源来处理本次导入任务。                                                                                                                                                                                                                                         |
| buffer-size             | 1024    | Integer  | 一次写入OceanBase的缓冲区大小。                                                                                                                                                                                                                                                             |
| max-error-rows          | 0       | Long     | 旁路导入任务最大可容忍的错误行数目。                                                                                                                                                                                                                                                               |
| dup-action              | REPLACE | String   | 旁路导入任务中主键重复时的处理策略。可以是 `STOP_ON_DUP`<br/>（本次导入失败），`REPLACE`<br/>（替换）或 `IGNORE`<br/>（忽略）。                                                                                                                                                                                          |
| timeout                 | 7d      | Duration | 旁路导入任务的超时时间。                                                                                                                                                                                                                                                                     |
| heartbeat-timeout       | 60s     | Duration | 旁路导入任务客户端的心跳超时时间。                                                                                                                                                                                                                                                                |
| heartbeat-interval      | 10s     | Duration | 旁路导入任务客户端的心跳间隔时间。                                                                                                                                                                                                                                                                |
| direct-load.load-method | full    | String   | 旁路导入导入模式：`full`, `inc`, `inc_replace`。<br/> `full`：全量旁路导入，默认值。<br/> `inc`：普通增量旁路导入，会进行主键冲突检查，observer-4.3.2及以上支持，暂时不支持direct-load.dup-action为REPLACE。<br/> `inc_replace`: 特殊replace模式的增量旁路导入，不会进行主键冲突检查，直接覆盖旧数据（相当于replace的效果），direct-load.dup-action参数会被忽略，observer-4.3.2及以上支持。 |
| enable-multi-node-write | false   | Boolean  | 是否启用支持多节点写入的旁路导入。默认不开启。                                                                                                                                                                                                                                                          |
| execution-id            |         | String   | 旁路导入任务的 execution id。仅当 `enable-multi-node-write`<br/>参数为true时生效。                                                                                                                                                                                                                |

# StarRocks Source

## 示例

从 StarRocks 读取数据同步到 OceanBase 的 Pipeline 可以定义如下：

```yaml
source:
  type: StarRocks
  jdbc-url : jdbc:mysql://localhost:9030/sys
  username: root
  password: ****
  scan-url: localhost:8030
  scan.max-retries: 1
  tables: test[1-2].orders[0-9]
  
routes:
  - source-table: test1.orders1
    sink-table: test1.order1
    description: sync orders table to order
  - source-table: test1.orders[23]
    sink-table: route.order
    description: sync orders table to route

oceanbase:
  url: jdbc:mysql://localhost:2881/test
  username: root@test
  password: ****
  schema-name: test

pipeline:
  name: Sync StarRocks Database to OceanBase
  parallelism: 2
```

## StarRocks Source配置项

|             参数              | 是否必填 |  数据类型  |                                                                       描述                                                                        |
|-----------------------------|------|--------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| type                        | 是    | STRING | 固定设置为 `starrocks`。                                                                                                                              |
| scan-url                    | 是    | STRING | FE 节点的连接地址，用于通过 Web 服务器访问 FE 节点。 格式如下：`<fe_host>:<fe_http_port>`。默认端口号为 `8030`。多个地址之间用逗号 (,) 分隔。例如 `192.168.xxx.xxx:8030,192.168.xxx.xxx:8030`。 |
| jdbc-url                    | 是    | STRING | FE 节点的连接地址，用于访问 FE 节点上的 MySQL 客户端。格式如下：`jdbc:mysql://<fe_host>:<fe_query_port>`。默认端口号为 `9030`。                                                  |
| username                    | 是    | STRING | 用于访问 StarRocks 集群的用户名。该账号需具备待读取数据的 StarRocks 表的读权限。                                                                                             |
| password                    | 是    | STRING | 用于访问 StarRocks 集群的用户密码。                                                                                                                         |
| database-name               | 是    | STRING | 待读取数据的 StarRocks 数据库的名称。                                                                                                                        |
| table-name                  | 是    | STRING | 待读取数据的 StarRocks 表的名称。                                                                                                                          |
| scan.connect.timeout-ms     | 否    | STRING | Flink Connector 连接 StarRocks 集群的时间上限。单位：毫秒。默认值：`1000`。超过该时间上限，则数据读取任务会报错。                                                                       |
| scan.params.keep-alive-min  | 否    | STRING | 数据读取任务的保活时间，通过轮询机制定期检查。单位：分钟。默认值：`10`。建议取值大于等于 `5`。                                                                                             |
| scan.params.query-timeout-s | 否    | STRING | 数据读取任务的超时时间，在任务执行过程中进行检查。单位：秒。默认值：`600`。如果超过该时间，仍未返回读取结果，则停止数据读取任务。                                                                             |
| scan.params.mem-limit-byte  | 否    | STRING | BE 节点中单个查询的内存上限。单位：字节。默认值：`1073741824`（即 1 GB）。                                                                                                 |
| scan.max-retries            | 否    | STRING | 数据读取失败时的最大重试次数。默认值：`1`。超过该数量上限，则数据读取任务报错。                                                                                                       |

## 注意事项

+ StarRocks 暂时不支持 checkpoint 机制。因此，如果读取任务失败，则无法保证数据一致性。读取任务失败后，StarRocks Source会重复拉取数据。
  - 对于非主键表并且没有唯一性约束的表，会导致数据重复。
    * 因此为了保证非主键表数据的一致性，可以关闭Flink Job的失败重试机制，Flink设置：restart-strategy: none。详见：[https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/execution/task_failure_recovery/](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/execution/task_failure_recovery/)
    * 一旦任务失败，可以手动清空表后，再次进行同步。
  - 对于主键表或者含有唯一性约束的表，由于OceanBase会自行去重，不会发生数据重复。

## 数据类型映射

|        StarRocks type        | OceanBase type |                      NOTE                      |
|------------------------------|----------------|------------------------------------------------|
| TINYINT                      | TINYINT        |                                                |
| SMALLINT                     | SMALLINT       |                                                |
| INT                          | INT            |                                                |
| BIGINT                       | BIGINT         |                                                |
| LARGEINT                     | BIGINT         |                                                |
| FLOAT                        | FLOAT          |                                                |
| DOUBLE                       | DOUBLE         |                                                |
| DECIMAL(p, s)                | DECIMAL(p, s)  |                                                |
| BOOLEAN                      | BOOLEAN        |                                                |
| STRING                       | VARCHAR(65535) |                                                |
| BINARY(n)                    | BINARY(n)      |                                                |
| VARBINARY(n)                 | VARBINARY(n)   |                                                |
| VARCHAR(n) where n <= 262144 | VARCHAR(n)     |                                                |
| VARCHAR(n) where n > 262144  | MEDIUMTEXT     |                                                |
| CHAR(n)                      | CHAR(n)        |                                                |
| DATE                         | DATE           |                                                |
| DATETIME                     | DATETIME       |                                                |
| JSON                         | JSON           |                                                |
| ARRAY                        | ARRAY          | 当ARRAY的嵌套深度小于等于6时。   注意：旁路导入数据同步方式暂不支持ARRAY类型。 |
| ARRAY                        | VARCHAR(65535) | 当ARRAY的嵌套深度大于6时                                |
| MAP                          | VARCHAR(65535) |                                                |
| SRTUCT                       |                | 暂不支持                                           |
| BITMAP                       |                | 暂不支持                                           |
| HLL                          |                | 暂不支持                                           |

