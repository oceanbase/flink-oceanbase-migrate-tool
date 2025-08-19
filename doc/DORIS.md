# Doris Source

[中文文档](DORIS_CN.md) | [English](DORIS.md)

## Example

A pipeline for reading data from Doris and writing to OceanBase can be defined as:

```yaml
source:
  type: doris
  jdbc-url: jdbc:mysql://localhost:9030
  username: root
  password: ""
  fenodes: localhost:8030
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
  password: 654321
  schema-name: test
  type: jdbc

pipeline:
  name: Doris to OceanBase Migration Test
  parallelism: 1
```

## Doris Source Configuration

| Parameter | Required | Data Type |                                                           Description                                                            |
|-----------|----------|-----------|----------------------------------------------------------------------------------------------------------------------------------|
| type      | Yes      | STRING    | Must be set to `doris`.                                                                                                     |
| jdbc-url  | Yes      | STRING    | Doris FE node JDBC connection address. The format is as follows: `jdbc:mysql://<host>:<port>`. The default port number is `9030`.            |
| username  | Yes      | STRING    | The username used to access Doris cluster. This account must have read permissions to the Doris table for the data to be read. |
| password  | Yes      | STRING    | User password to access Doris cluster.                                                                                              |
| fenodes   | Yes      | STRING    | Doris FE node HTTP service address for accessing FE node through web server. Format: `<host>:<port>`, default port is `8030`. |
| tables    | Yes      | STRING    | Source table name. Supports regular expression matching, e.g., `database[0-9].table[0-9]`.                                               |

## Data Type Mapping

| Doris type                     | OceanBase type |     Notes     |
|-------------------------------------|----------------|---------------|
| BOOLEAN                             | BOOLEAN        |               |
| TINYINT                             | TINYINT        |               |
| SMALLINT                            | SMALLINT       |               |
| INT                                 | INT            |               |
| BIGINT                              | BIGINT         |               |
| LARGEINT                            | BIGINT         |               |
| FLOAT                               | FLOAT          |               |
| DOUBLE                              | DOUBLE         |               |
| DECIMAL/DECIMALV2                   | DECIMAL        |               |
| CHAR                                | CHAR           |               |
| VARCHAR                             | VARCHAR        |               |
| STRING                              | VARCHAR        |               |
| BINARY                              | BINARY         |               |
| VARBINARY                           | VARBINARY      |               |
| DATE                                | DATE           |               |
| DATETIME                            | DATETIME       |               |
| JSON                                | JSON           |               |
| ARRAY                               | ARRAY          | When ARRAY nesting depth is less than or equal to 6. Note: Direct load data synchronization method does not support ARRAY type. |
| ARRAY                               | VARCHAR(65535) | When ARRAY nesting depth is greater than 6                                |
| MAP                                 | VARCHAR(65535) |  |
