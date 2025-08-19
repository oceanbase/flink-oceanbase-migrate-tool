# ClickHouse Source

[中文文档](CLICKHOUSE_CN.md) | [English](CLICKHOUSE.md)

## Example

A pipeline for reading data from ClickHouse and writing to OceanBase can be defined as:

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

## ClickHouse Source Configuration

| Parameter | Required | Data Type |                                                           Description                                                            |
|-----------|----------|-----------|----------------------------------------------------------------------------------------------------------------------------------|
| type      | Yes      | STRING    | Must be set to `ClickHouse`.                                                                                                     |
| jdbc-url  | Yes      | STRING    | ClickHouse jdbc url . The format is as follows: `jdbc:clickhouse://<host>:<port>`. The default port number is `8123`.            |
| username  | Yes      | STRING    | The username used to access ClickHouse. This account must have read permissions to the ClickHouse table for the data to be read. |
| password  | Yes      | STRING    | User password to access ClickHouse.                                                                                              |
| database  | Yes      | STRING    | Source database name.                                                                                                            |
| tables    | Yes      | STRING    | Source table name.                                                                                                               |

## Data Type Mapping

| ClickHouse type                     | OceanBase type |     Notes     |
|-------------------------------------|----------------|---------------|
| String/FixedString/ IP/UUID/Enum    | VARBINARY(n)   |               |
| Int8                                | TINYINT        |               |
| INT                                 | INT            |               |
| Int16/UInt8                         | SMALLINT       |               |
| Int32/UInt16                        | BIGINT         |               |
| Int64/UInt32/UInt64                 | FLOAT          |               |
| Int128/nt256/UInt64/UInt128/UInt256 | DECIMAL(38, 0) |               |
| Float32                             | FLOAT          |               |
| Float64                             | DOUBLE         |               |
| Decimal                             | DECIMAL        |               |
| Date                                | DATE           |               |
| DateTime/DateTime32/DateTime64      | DATETIME       |               |
| ARRAY                               |                | Not supported |
| MAP                                 |                | Not supported |
| Tuple                               |                | Not supported |
| Nested                              |                | Not supported |
| AggregateFuntion                    |                | Not supported |

