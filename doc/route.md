# 定义 
  
**Route**指定了匹配源表列表并映射到目的表表的规则。最典型的场景是分库分表的合并，将多个上游源表路由到同一个目的表。

# 参数 
  
要描述一条Route，需要满足以下条件：

| parameter | 意义 | 可选/必需 |
| --- | --- | --- |
| source-table  | 源表id，支持正则表达式 | 必需的 |
| sink-table   | Sink表id | 必需的 |
| description | 路由规则说明 | 可选的 |


  
路由模块可以包含源表/接收器表规则的列表。

# 示例
## 将一张数据源表路由到一张数据接收器表 
如果将数据库`mydb`中的表`web_order`同步到 OB 表`ods_web_order` ，我们可以使用这个 yaml 文件来定义这条路由：

```yaml
route:
    source-table: mydb.web_order
    sink-table: mydb.ods_web_order
    description: sync table to one destination table with given prefix ods_
```

## 将多个数据源表路由到一个数据接收器表
另外，如果要将数据库`mydb`中的sharding表同步到 OB 表`ods_web_order` ，我们可以使用这个 yaml 文件来定义这个路由：

```yaml
route:
    source-table: mydb.ods_web_order[0-9]
    sink-table: mydb.ods_web_order
    description: sync sharding tables to one destination table
```

## 通过组合路由规则实现复杂路由
另外，如果你想指定多种不同的映射规则，我们可以使用这个yaml文件来定义这个路由：

```yaml
route:
  - source-table: mydb.orders
    sink-table: ods_db.ods_orders
    description: sync orders table to orders
  - source-table: mydb.shipments
    sink-table: ods_db.ods_shipments
    description: sync shipments table to ods_shipments
  - source-table: mydb.products
    sink-table: ods_db.ods_products
    description: sync products table to ods_products
```
