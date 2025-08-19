# ClickHouse Source

[English](CLICKHOUSE.md) | [中文文档](CLICKHOUSE_CN.md)

## 示例

从 ClickHouse 读取数据同步到 OceanBase 的 Pipeline 可以定义如下：

```yaml
source:
  type: ClickHouse
  jdbc-url : jdbc:clickhouse://localhost:8123
  username: root
  password: ****
  parallelism: 4
  database: test
  tables: order

routes:
  - source-table: test1.orders1
    sink-table: test1.order1
    description: sync orders table to order
  - source-table: test1.orders[23]
    sink-table: route.order
    description: sync orders table to route

oceanbase:
  url: jdbc:mysql://localhouse:2810
  username: root@sun
  password: ****
  schema-name: test1

pipeline:
  name: Test sync ClickHouse to OB
  parallelism: 2
```

## ClickHouse Source配置项

|    参数    | 是否必填 |  数据类型  |                                     描述                                     |
|----------|------|--------|----------------------------------------------------------------------------|
| type     | 是    | STRING | 固定设置为 `ClickHouse`。                                                        |
| jdbc-url | 是    | STRING | ClickHouse jdbc url 。格式如下：`jdbc:clickhouse://<host>:<port>`。默认端口号为 `8123`。 |
| username | 是    | STRING | 用于访问 ClickHouse 的用户名。该账号需具备待读取数据的 ClickHouse表的读权限。。                        |
| password | 是    | STRING | 用于访问 ClickHouse的用户密码。                                                      |
| database | 是    | STRING | 待读取数据的 ClickHouse 数据库的名称。                                                  |
| tables   | 是    | STRING | 待读取数据的 ClickHouse表的名称。                                                     |

## 数据类型映射

|           ClickHouse type           | OceanBase type | NOTE |
|-------------------------------------|----------------|------|
| String/FixedString/ IP/UUID/Enum    | VARBINARY(n)   |      |
| Int8                                | TINYINT        |      |
| INT                                 | INT            |      |
| Int16/UInt8                         | SMALLINT       |      |
| Int32/UInt16                        | BIGINT         |      |
| Int64/UInt32/UInt64                 | FLOAT          |      |
| Int128/nt256/UInt64/UInt128/UInt256 | DECIMAL(38, 0) |      |
| Float32                             | FLOAT          |      |
| Float64                             | DOUBLE         |      |
| Decimal                             | DECIMAL        |      |
| Date                                | DATE           |      |
| DateTime/DateTime32/DateTime64      | DATETIME       |      |
| ARRAY                               |                | 暂不支持 |
| MAP                                 |                | 暂不支持 |
| Tuple                               |                | 暂不支持 |
| Nested                              |                | 暂不支持 |
| AggregateFuntion                    |                | 暂不支持 |