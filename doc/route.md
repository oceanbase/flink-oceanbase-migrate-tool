# Route Definition

[English](route.md) | [中文文档](route_CN.md)

**Route** specifies rules for matching source table lists and mapping them to destination tables. The most typical scenario is the merging of sharded databases and tables, routing multiple upstream source tables to the same destination table.

# Parameters

To describe a Route, the following conditions must be met:

| Parameter | Meaning | Optional/Required |
|-----------|---------|-------------------|
| source-table | Source table id, supports regular expressions | Required |
| sink-table | Sink table id | Required |
| description | Route rule description | Optional |

The routing module can contain a list of source table/sink table rules.

# Examples

## Route One Source Table to One Sink Table

If you want to synchronize table `web_order` from database `mydb` to OB table `ods_web_order`, you can use this yaml file to define this route:

```yaml
route:
    source-table: mydb.web_order
    sink-table: mydb.ods_web_order
    description: sync table to one destination table with given prefix ods_
```

## Route Multiple Source Tables to One Sink Table

Additionally, if you want to synchronize sharding tables from database `mydb` to OB table `ods_web_order`, you can use this yaml file to define this route:

```yaml
route:
    source-table: mydb.ods_web_order[0-9]
    sink-table: mydb.ods_web_order
    description: sync sharding tables to one destination table
```

## Implement Complex Routing Through Combined Route Rules

Additionally, if you want to specify multiple different mapping rules, you can use this yaml file to define this route:

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
