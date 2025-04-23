# flink-oceanbase-migrate-tool

English | [简体中文](README.md)

# Project Introduction

flink-oceanbase-migrate-tool (abbreviated as Flink-OMT) is a data migration tool provided by OceanBase for importing data from source databases into OceanBase through Flink. This tool allows users to elegantly define their data synchronization workflows using YAML configuration files, automatically generating customized Flink operators and submitting Flink jobs.  
Flink-OMT is deeply integrated and powered by Apache Flink, offering the following core features:

+ ✅ End-to-end data integration framework
+ ✅ Automatic table schema synchronization capability
+ ✅ Support for multi-database and multi-table synchronization
+ ✅ [Support for multi-database and multi-table routing synchronization](./doc/route.md)

Currently, Flink-OMT only supports StarRocks as the source database.

# Compilation and Build

```shell
git clone https://github.com/oceanbase/flink-oceanbase-migrate-tool.git
cd flink-oceanbase-migrate-tool
mvn clean package -DskipTests

# Or specify the flink version to build
mvn clean package -DskipTests -Dflink.majorVersion=1.19 -Dflink.version=1.19.1
```

# Usage

```shell
<FLINK_HOME>bin/flink run \
     -D execution.checkpointing.interval=10s\
     -D parallelism.default=1\
     -c com.oceanbase.omt.cli.CommandLineCliFront\
     lib/flink-omt-1.0-SNAPSHOT.jar \
     -config config.yaml
```

# OceanBase Sink

OceanBase Sink supports synchronizing data to OceanBase via JDBC or direct-load methods.

+ The implementation of OceanBase Sink depends on the [flink-connector-oceanbase](https://github.com/oceanbase/flink-connector-oceanbase) project.
+ For OceanBase's direct-load feature, refer to the [Direct-Load Documentation](https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000001428636)

## Examples

+ JDBC Method

```yaml
oceanbase:
  url: jdbc:mysql://localhost:2881/test
  username: root@test
  password: ****
  schema-name: test
```

+ Direct-Load Method

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

## OceanBase Sink Configuration Options

### JDBC Method Configuration

+ Implementation depends on [flink-connector-oceanbase](https://github.com/oceanbase/flink-connector-oceanbase). Refer to documentation: [https://github.com/oceanbase/flink-connector-oceanbase/blob/main/docs/sink/flink-connector-oceanbase_cn.md](https://github.com/oceanbase/flink-connector-oceanbase/blob/main/docs/sink/flink-connector-oceanbase_cn.md)

|            Parameter Name            |      Default Value       |   Type   |                                                  Description                                                   |
|--------------------------------------|--------------------------|----------|----------------------------------------------------------------------------------------------------------------|
| type                                 | jdbc                     | String   | OceanBase Sink synchronization method. Supported values: `jdbc` and `direct-load`. Default: `jdbc`.            |
| url                                  |                          | String   | JDBC URL of the database.                                                                                      |
| username                             |                          | String   | Connection username.                                                                                           |
| password                             |                          | String   | Connection password.                                                                                           |
| schema-name                          |                          | String   | Schema name or database name.                                                                                  |
| table-name                           |                          | String   | Table name.                                                                                                    |
| driver-class-name                    | com.mysql.cj.jdbc.Driver | String   | JDBC driver class name. Default: `com.mysql.cj.jdbc.Driver`. Requires manual dependency management if changed. |
| druid-properties                     |                          | String   | Druid connection pool properties, separated by semicolons.                                                     |
| sync-write                           | false                    | Boolean  | Enable synchronous write. When `true`, writes directly to database without buffering.                          |
| buffer-flush.interval                | 1s                       | Duration | Buffer flush interval. Set to `0` to disable periodic flushing.                                                |
| buffer-flush.buffer-size             | 1000                     | Integer  | Buffer size.                                                                                                   |
| max-retries                          | 3                        | Integer  | Maximum retry attempts on failure.                                                                             |
| memstore-check.enabled               | true                     | Boolean  | Enable memory usage check.                                                                                     |
| memstore-check.threshold             | 0.9                      | Double   | Memory usage threshold ratio relative to maximum limit.                                                        |
| memstore-check.interval              | 30s                      | Duration | Memory check interval.                                                                                         |
| partition.enabled                    | false                    | Boolean  | Enable partition calculation. Only effective when both `sync-write` and `direct-load.enabled` are `false`.     |
| table.oracle-tenant-case-insensitive | true                     | Boolean  | By default, schema and column names are case-insensitive for Oracle tenants.                                   |

### Direct-Load Configuration

+ Implementation depends on [flink-connector-oceanbase](https://github.com/oceanbase/flink-connector-oceanbase) direct-load module. Refer to documentation: [https://github.com/oceanbase/flink-connector-oceanbase/blob/main/docs/sink/flink-connector-oceanbase-directload_cn.md](https://github.com/oceanbase/flink-connector-oceanbase/blob/main/docs/sink/flink-connector-oceanbase-directload_cn.md)

|     Parameter Name      | Default Value |   Type   |                                                                                                   Description                                                                                                    |
|-------------------------|---------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type                    | jdbc          | String   | OceanBase Sink synchronization method. Supported values: `jdbc` and `direct-load`. Default: `jdbc`.                                                                                                              |
| url                     |               | String   | JDBC URL for schema synchronization. Direct-load data synchronization does not depend on JDBC.                                                                                                                   |
| username                |               | String   | Connection username.                                                                                                                                                                                             |
| password                |               | String   | Connection password.                                                                                                                                                                                             |
| host                    |               | String   | OceanBase database host address.                                                                                                                                                                                 |
| port                    |               | Integer  | RPC port for direct-load.                                                                                                                                                                                        |
| tenant-name             |               | String   | Tenant name.                                                                                                                                                                                                     |
| schema-name             |               | String   | Schema name or database name.                                                                                                                                                                                    |
| table-name              |               | String   | Table name.                                                                                                                                                                                                      |
| parallel                | 8             | Integer  | Server-side concurrency for direct-load. Determines CPU resources allocated for the import task.                                                                                                                 |
| buffer-size             | 1024          | Integer  | Buffer size for single write operation to OceanBase.                                                                                                                                                             |
| max-error-rows          | 0             | Long     | Maximum allowed error rows for direct-load task.                                                                                                                                                                 |
| dup-action              | REPLACE       | String   | Conflict resolution policy for duplicate primary keys: `STOP_ON_DUP` (fail), `REPLACE` (replace), or `IGNORE` (ignore).                                                                                          |
| timeout                 | 7d            | Duration | Timeout for direct-load task.                                                                                                                                                                                    |
| heartbeat-timeout       | 60s           | Duration | Client heartbeat timeout for direct-load task.                                                                                                                                                                   |
| heartbeat-interval      | 10s           | Duration | Client heartbeat interval for direct-load task.                                                                                                                                                                  |
| direct-load.load-method | full          | String   | Direct-load mode: `full` (full import, default), `inc` (incremental with conflict checks, requires observer-4.3.2+), `inc_replace` (replace-mode incremental without conflict checks, requires observer-4.3.2+). |
| enable-multi-node-write | false         | Boolean  | Enable multi-node write support for direct-load. Default: `false`.                                                                                                                                               |
| execution-id            |               | String   | Direct-load task execution ID. Only effective when `enable-multi-node-write` is `true`.                                                                                                                          |

# StarRocks Source

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

|          Parameter          | Required | Data Type |                                                                                 Description                                                                                 |
|-----------------------------|----------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type                        | Yes      | String    | Must be set to `starrocks`.                                                                                                                                                 |
| scan-url                    | Yes      | String    | FE node HTTP address in format `<fe_host>:<fe_http_port>`. Default port: `8030`. Multiple addresses separated by commas, e.g., `192.168.xxx.xxx:8030,192.168.xxx.xxx:8030`. |
| jdbc-url                    | Yes      | String    | FE node MySQL client address in format `jdbc:mysql://<fe_host>:<fe_query_port>`. Default port: `9030`.                                                                      |
| username                    | Yes      | String    | StarRocks username with read permissions.                                                                                                                                   |
| password                    | Yes      | String    | StarRocks user password.                                                                                                                                                    |
| database-name               | Yes      | String    | Source database name.                                                                                                                                                       |
| table-name                  | Yes      | String    | Source table name.                                                                                                                                                          |
| scan.connect.timeout-ms     | No       | String    | Connection timeout (ms). Default: `1000`.                                                                                                                                   |
| scan.params.keep-alive-min  | No       | String    | Task keep-alive time (minutes). Default: `10`. Recommended ≥5.                                                                                                              |
| scan.params.query-timeout-s | No       | String    | Query timeout (seconds). Default: `600`.                                                                                                                                    |
| scan.params.mem-limit-byte  | No       | String    | Memory limit per query on BE nodes (bytes). Default: `1073741824` (1GB).                                                                                                    |
| scan.max-retries            | No       | String    | Maximum retry attempts on read failure. Default: `1`.                                                                                                                       |

## Notes

+ StarRocks currently does not support checkpoint mechanism. Therefore, if a read task fails, data consistency cannot be guaranteed. Failed tasks will retry and may cause data duplication:
  - For tables without primary keys or unique constraints, this may lead to duplicate data.
    * To ensure consistency for such tables:
      - Disable Flink job retries by setting `restart-strategy: none`. See: [Flink Restart Strategies](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/execution/task_failure_recovery/)
      - Manually truncate tables and restart synchronization after failures.
  - For tables with primary keys or unique constraints, OceanBase will automatically deduplicate.

## Data Type Mapping

|       StarRocks Type        | OceanBase Type |                                         Notes                                         |
|-----------------------------|----------------|---------------------------------------------------------------------------------------|
| TINYINT                     | TINYINT        |                                                                                       |
| SMALLINT                    | SMALLINT       |                                                                                       |
| INT                         | INT            |                                                                                       |
| BIGINT                      | BIGINT         |                                                                                       |
| LARGEINT                    | BIGINT         |                                                                                       |
| FLOAT                       | FLOAT          |                                                                                       |
| DOUBLE                      | DOUBLE         |                                                                                       |
| DECIMAL(p, s)               | DECIMAL(p, s)  |                                                                                       |
| BOOLEAN                     | BOOLEAN        |                                                                                       |
| STRING                      | VARCHAR(65535) |                                                                                       |
| BINARY(n)                   | BINARY(n)      |                                                                                       |
| VARBINARY(n)                | VARBINARY(n)   |                                                                                       |
| VARCHAR(n) where n ≤ 262144 | VARCHAR(n)     |                                                                                       |
| VARCHAR(n) where n > 262144 | MEDIUMTEXT     |                                                                                       |
| CHAR(n)                     | CHAR(n)        |                                                                                       |
| DATE                        | DATE           |                                                                                       |
| DATETIME                    | DATETIME       |                                                                                       |
| JSON                        | JSON           |                                                                                       |
| ARRAY                       | ARRAY          | Supported when array nesting depth ≤6. Note: Direct-load does not support ARRAY type. |
| ARRAY                       | VARCHAR(65535) | When array nesting depth >6.                                                          |
| MAP                         | VARCHAR(65535) |                                                                                       |
| STRUCT                      |                | Not supported                                                                         |
| BITMAP                      |                | Not supported                                                                         |
| HLL                         |                | Not supported                                                                         |

