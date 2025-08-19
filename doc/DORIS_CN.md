# Doris Source

[English](DORIS.md) | [中文文档](DORIS_CN.md)

## 示例

从 Doris 读取数据同步到 OceanBase 的 Pipeline 可以定义如下：

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

## Doris Source 配置项

|    参数    | 是否必填 |  数据类型  |                                     描述                                     |
|----------|------|--------|----------------------------------------------------------------------------|
| type     | 是    | STRING | 固定设置为 `doris`。                                                        |
| jdbc-url | 是    | STRING | Doris FE 节点的 JDBC 连接地址。格式如下：`jdbc:mysql://<host>:<port>`。默认端口号为 `9030`。 |
| username | 是    | STRING | 用于访问 Doris 集群的用户名。该账号需具备待读取数据的 Doris 表的读权限。                        |
| password | 是    | STRING | 用于访问 Doris 集群的用户密码。                                                      |
| fenodes  | 是    | STRING | Doris FE 节点的 HTTP 服务地址，用于通过 Web 服务器访问 FE 节点。格式：`<host>:<port>`，默认端口为 `8030`。 |
| tables   | 是    | STRING | 待读取数据的 Doris 表的名称。支持正则表达式匹配，如：`database[0-9].table[0-9]`。 |

## 数据类型映射

|           Doris type           | OceanBase type | NOTE |
|-------------------------------------|----------------|------|
| BOOLEAN                             | BOOLEAN        |      |
| TINYINT                             | TINYINT        |      |
| SMALLINT                            | SMALLINT       |      |
| INT                                 | INT            |      |
| BIGINT                              | BIGINT         |      |
| LARGEINT                            | BIGINT         |      |
| FLOAT                               | FLOAT          |      |
| DOUBLE                              | DOUBLE         |      |
| DECIMAL/DECIMALV2                   | DECIMAL        |      |
| CHAR                                | CHAR           |      |
| VARCHAR                             | VARCHAR        |      |
| STRING                              | VARCHAR        |      |
| BINARY                              | BINARY         |      |
| VARBINARY                           | VARBINARY      |      |
| DATE                                | DATE           |      |
| DATETIME                            | DATETIME       |      |
| JSON                                | JSON           |      |
| ARRAY                               | ARRAY          | 当ARRAY的嵌套深度小于等于6时。   注意：旁路导入数据同步方式暂不支持ARRAY类型。 |
| ARRAY                               | VARCHAR(65535) | 当ARRAY的嵌套深度大于6时                                |
| MAP                                 | VARCHAR(65535) |  |

