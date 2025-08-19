# StarRocks Source

[中文文档](STARROCKS_CN.md) | [English](STARROCKS.md)

## Example

A pipeline for reading data from StarRocks and writing to OceanBase can be defined as:

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

## StarRocks Source Configuration

| Parameter | Required | Data Type |                                                           Description                                                            |
|-----------|----------|-----------|----------------------------------------------------------------------------------------------------------------------------------|
| type      | Yes      | STRING    | Must be set to `starrocks`.                                                                                                     |
| scan-url  | Yes      | STRING    | FE node connection address for accessing FE node through web server. Format: `<fe_host>:<fe_http_port>`. Default port is `8030`. Multiple addresses can be separated by commas (,). Example: `192.168.xxx.xxx:8030,192.168.xxx.xxx:8030`. |
| jdbc-url  | Yes      | STRING    | FE node connection address for accessing MySQL client on FE node. Format: `jdbc:mysql://<fe_host>:<fe_query_port>`. Default port is `9030`. |
| username  | Yes      | STRING    | The username used to access StarRocks cluster. This account must have read permissions to the StarRocks table for the data to be read. |
| password  | Yes      | STRING    | User password to access StarRocks cluster.                                                                                              |
| database-name | Yes   | STRING    | Source database name.                                                                                                            |
| table-name    | Yes   | STRING    | Source table name.                                                                                                               |
| scan.connect.timeout-ms | No | STRING | Time limit for Flink Connector to connect to StarRocks cluster. Unit: milliseconds. Default: `1000`. If exceeded, data reading task will fail. |
| scan.params.keep-alive-min | No | STRING | Keep-alive time for data reading task, checked periodically through polling mechanism. Unit: minutes. Default: `10`. Recommended value >= `5`. |
| scan.params.query-timeout-s | No | STRING | Timeout for data reading task, checked during task execution. Unit: seconds. Default: `600`. If exceeded without returning results, data reading task will stop. |
| scan.params.mem-limit-byte | No | STRING | Memory limit for single query on BE node. Unit: bytes. Default: `1073741824` (1 GB). |
| scan.max-retries | No | STRING | Maximum retry count when data reading fails. Default: `1`. If exceeded, data reading task will fail. |

## Important Notes

+ StarRocks currently does not support checkpoint mechanism. Therefore, if the reading task fails, data consistency cannot be guaranteed. After the reading task fails, StarRocks Source will repeatedly pull data.
  - For tables without primary keys and without unique constraints, this will cause data duplication.
    * To ensure data consistency for non-primary key tables, you can disable Flink Job's failure retry mechanism by setting: restart-strategy: none. See: [https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/execution/task_failure_recovery/](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/execution/task_failure_recovery/)
    * Once the task fails, you can manually clear the table and perform synchronization again.
  - For tables with primary keys or unique constraints, OceanBase will automatically deduplicate, so data duplication will not occur.

## Data Type Mapping

| StarRocks type | OceanBase type | Notes |
|----------------|----------------|-------|
| TINYINT | TINYINT | |
| SMALLINT | SMALLINT | |
| INT | INT | |
| BIGINT | BIGINT | |
| LARGEINT | BIGINT | |
| FLOAT | FLOAT | |
| DOUBLE | DOUBLE | |
| DECIMAL(p, s) | DECIMAL(p, s) | |
| BOOLEAN | BOOLEAN | |
| STRING | VARCHAR(65535) | |
| BINARY(n) | BINARY(n) | |
| VARBINARY(n) | VARBINARY(n) | |
| VARCHAR(n) where n <= 262144 | VARCHAR(n) | |
| VARCHAR(n) where n > 262144 | MEDIUMTEXT | |
| CHAR(n) | CHAR(n) | |
| DATE | DATE | |
| DATETIME | DATETIME | |
| JSON | JSON | |
| ARRAY | ARRAY | When ARRAY nesting depth is less than or equal to 6. Note: Direct load data synchronization method does not support ARRAY type. |
| ARRAY | VARCHAR(65535) | When ARRAY nesting depth is greater than 6 |
| MAP | VARCHAR(65535) | |
| STRUCT | | Not supported |
| BITMAP | | Not supported |
| HLL | | Not supported |
