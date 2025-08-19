# flink-oceanbase-migrate-tool

[English](README.md) | [简体中文](README_CN.md)

# Project Introduction

flink-oceanbase-migrate-tool (abbreviated as Flink-OMT) is a data migration tool provided by OceanBase for importing data from source databases to OceanBase through Flink. This tool allows users to elegantly define their data synchronization workflows in YAML configuration files, automatically generate customized Flink operators, and submit Flink jobs.

Flink-OMT is deeply integrated and driven by Apache Flink, providing the following core features:

+ ✅ End-to-end data integration framework
+ ✅ Automatic table structure synchronization capability
+ ✅ Support for multi-database and multi-table synchronization
+ ✅ [Support for multi-database and multi-table routing synchronization](./doc/route.md)

Currently, Flink-OMT supports the following data sources:

## Supported Data Sources

|  Data Source   |   Status    |                         Documentation                          |
|----------------|-------------|----------------------------------------------------------------|
| **StarRocks**  | ✅ Supported | [English](./doc/STARROCKS.md) \| [中文](./doc/STARROCKS_CN.md)   |
| **ClickHouse** | ✅ Supported | [English](./doc/CLICKHOUSE.md) \| [中文](./doc/CLICKHOUSE_CN.md) |
| **Doris**      | ✅ Supported | [English](./doc/DORIS.md) \| [中文](./doc/DORIS_CN.md)           |

Each data source provides detailed configuration instructions, data type mappings, and usage examples. Click the corresponding documentation links for detailed information.

# Build and Compilation

```shell
git clone https://github.com/oceanbase/flink-oceanbase-migrate-tool.git
cd flink-oceanbase-migrate-tool
mvn clean package -DskipTests

# Or specify the flink version to build
mvn clean package -DskipTests -Dflink.majorVersion=1.19 -Dflink.version=1.19.1
```

# Quick Start

## Step 1: Write Configuration File

First, create a YAML configuration file, for example `config.yaml`:

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

> 💡 **Tip**: For detailed configuration instructions, please refer to the corresponding data source documentation: [StarRocks](./doc/STARROCKS.md) | [ClickHouse](./doc/CLICKHOUSE.md) | [Doris](./doc/DORIS.md)

## Step 2: Submit and Run

Use the following command to submit the Flink job:

```shell
<FLINK_HOME>/bin/flink run \
     -D execution.checkpointing.interval=10s \
     -D parallelism.default=1 \
     -c com.oceanbase.omt.cli.CommandLineCliFront \
     lib/flink-omt-1.0-SNAPSHOT.jar \
     -config config.yaml
```

### Parameter Description

- `execution.checkpointing.interval=10s`: Set checkpoint interval to 10 seconds
- `parallelism.default=1`: Set default parallelism to 1
- `-config config.yaml`: Specify configuration file path

# OceanBase Sink

OceanBase Sink supports synchronizing data to OceanBase via JDBC or direct load methods.

+ The implementation of OceanBase Sink depends on the [flink-connector-oceanbase](https://github.com/oceanbase/flink-connector-oceanbase) project.
+ For OceanBase's direct load functionality, see [Direct Load Documentation](https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000001428636)

## Examples

+ JDBC Method

```yaml
oceanbase:
  url: jdbc:mysql://localhost:2881/test
  username: root@test
  password: ****
  schema-name: test
```

+ Direct Load Method

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

## OceanBase Sink Configuration

### JDBC Method Configuration

+ Implementation depends on the [flink-connector-oceanbase](https://github.com/oceanbase/flink-connector-oceanbase) project. For reference documentation, see: [https://github.com/oceanbase/flink-connector-oceanbase/blob/main/docs/sink/flink-connector-oceanbase_cn.md](https://github.com/oceanbase/flink-connector-oceanbase/blob/main/docs/sink/flink-connector-oceanbase_cn.md)

|            Parameter Name            |      Default Value       |   Type   |                                                                      Description                                                                      |
|--------------------------------------|--------------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| type                                 | jdbc                     | String   | OceanBase Sink data synchronization method, supports jdbc and direct-load, default is jdbc.                                                           |
| url                                  |                          | String   | Database JDBC url.                                                                                                                                    |
| username                             |                          | String   | Connection username.                                                                                                                                  |
| password                             |                          | String   | Connection password.                                                                                                                                  |
| schema-name                          |                          | String   | Connected schema name or db name.                                                                                                                     |
| table-name                           |                          | String   | Table name.                                                                                                                                           |
| driver-class-name                    | com.mysql.cj.jdbc.Driver | String   | Driver class name, default is 'com.mysql.cj.jdbc.Driver'. If other values are set, corresponding dependencies need to be manually introduced.         |
| druid-properties                     |                          | String   | Druid connection pool properties, multiple values separated by semicolons.                                                                            |
| sync-write                           | false                    | Boolean  | Whether to enable synchronous writing. When set to true, data will be written directly to the database without using buffer.                          |
| buffer-flush.interval                | 1s                       | Duration | Buffer refresh cycle. Set to '0' to disable periodic refresh.                                                                                         |
| buffer-flush.buffer-size             | 1000                     | Integer  | Buffer size.                                                                                                                                          |
| max-retries                          | 3                        | Integer  | Number of retries on failure.                                                                                                                         |
| memstore-check.enabled               | true                     | Boolean  | Whether to enable memory check.                                                                                                                       |
| memstore-check.threshold             | 0.9                      | Double   | Proportion of memory usage threshold relative to maximum limit value.                                                                                 |
| memstore-check.interval              | 30s                      | Duration | Memory usage check cycle.                                                                                                                             |
| partition.enabled                    | false                    | Boolean  | Whether to enable partition calculation function, write data by partition. Only effective when both 'sync-write' and 'direct-load.enabled' are false. |
| table.oracle-tenant-case-insensitive | true                     | Boolean  | By default, in Oracle tenant, Schema names and column names are case-insensitive.                                                                     |

### Direct Load Configuration

+ Implementation depends on the direct load module of [flink-connector-oceanbase](https://github.com/oceanbase/flink-connector-oceanbase) project. For reference documentation, see: [https://github.com/oceanbase/flink-connector-oceanbase/blob/main/docs/sink/flink-connector-oceanbase-directload_cn.md](https://github.com/oceanbase/flink-connector-oceanbase/blob/main/docs/sink/flink-connector-oceanbase-directload_cn.md)

|     Parameter Name      | Default Value |   Type   |                                                                                                                                                                                                                                                                      Description                                                                                                                                                                                                                                                                      |
|-------------------------|---------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type                    | jdbc          | String   | OceanBase Sink data synchronization method, supports jdbc and direct-load, default is jdbc.                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| url                     |               | String   | Database JDBC url. User table structure synchronization, direct load data synchronization itself does not depend on jdbc.                                                                                                                                                                                                                                                                                                                                                                                                                             |
| username                |               |          | Connection username.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| password                |               | String   | Password.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| host                    |               | String   | OceanBase database host address.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| port                    |               | Integer  | RPC port used by direct load.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| tenant-name             |               | String   | Tenant name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| schema-name             |               | String   | Schema name or DB name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| table-name              |               | String   | Table name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| parallel                | 8             | Integer  | Direct load server-side concurrency. This parameter determines how many CPU resources the server uses to process this import task.                                                                                                                                                                                                                                                                                                                                                                                                                    |
| buffer-size             | 1024          | Integer  | Buffer size for one-time write to OceanBase.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| max-error-rows          | 0             | Long     | Maximum number of error rows tolerated by direct load task.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| dup-action              | REPLACE       | String   | Processing strategy for primary key duplication in direct load task. Can be `STOP_ON_DUP` (import fails), `REPLACE` (replace) or `IGNORE` (ignore).                                                                                                                                                                                                                                                                                                                                                                                                   |
| timeout                 | 7d            | Duration | Timeout for direct load task.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| heartbeat-timeout       | 60s           | Duration | Heartbeat timeout for direct load task client.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| heartbeat-interval      | 10s           | Duration | Heartbeat interval for direct load task client.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| direct-load.load-method | full          | String   | Direct load import mode: `full`, `inc`, `inc_replace`. `full`: Full direct load, default value. `inc`: Normal incremental direct load, will perform primary key conflict check, supported by observer-4.3.2 and above, temporarily does not support direct-load.dup-action as REPLACE. `inc_replace`: Special replace mode incremental direct load, will not perform primary key conflict check, directly overwrite old data (equivalent to replace effect), direct-load.dup-action parameter will be ignored, supported by observer-4.3.2 and above. |
| enable-multi-node-write | false         | Boolean  | Whether to enable multi-node write support for direct load. Default is not enabled.                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| execution-id            |               | String   | Execution id for direct load task. Only effective when `enable-multi-node-write` parameter is true.                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

