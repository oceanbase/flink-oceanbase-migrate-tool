# StarRocks Source

[English](STARROCKS.md) | [中文文档](STARROCKS_CN.md)

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

## StarRocks Source 配置项

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
