-- Copyright 2024 OceanBase.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

CREATE TABLE IF NOT EXISTS test1.orders1 (
  order_id     INT default '1' comment 'order id',
  order_date   DATETIME,
  customer_name VARCHAR(225) default 'default',
  price        double,
  product_id   INT,
  order_status BOOLEAN
)
DISTRIBUTED BY HASH(order_id)
PROPERTIES (
    "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS test1.orders2 (
  order_id      INT COMMENT 'order id',
  order_date    DATETIME,
  customer_name VARCHAR(65533), -- errCode = 2, detailMessage = VARCHAR size must be <= 65533: 1048576
  price         DOUBLE,
  product_id    INT,
  order_status  INT DEFAULT '0'
)
UNIQUE KEY (order_id)
DISTRIBUTED BY HASH(order_id)
PROPERTIES (
    "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS test1.orders3 (
  order_id      INT COMMENT 'order id',
  order_date    DATETIME,
  customer_name VARCHAR(65533), -- errCode = 2, detailMessage = VARCHAR size must be <= 65533: 1048576
  price         DOUBLE,
  product_id    INT,
  order_status  INT DEFAULT '0'
)
UNIQUE KEY (order_id)
DISTRIBUTED BY HASH(order_id)
PROPERTIES (
    "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS test2.orders3
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    V0 DATETIME DEFAULT CURRENT_TIMESTAMP,
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE = olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1)
(
    PARTITION p1 VALUES LESS THAN ("2014-01-01"),
    PARTITION p2 VALUES LESS THAN ("2014-06-01"),
    PARTITION p3 VALUES LESS THAN ("2014-12-01")
)
DISTRIBUTED BY HASH(k2)
PROPERTIES(
    "replication_num" = "1",
    "storage_cooldown_time" = "2026-12-31 23:59:59"
);

CREATE TABLE IF NOT EXISTS test2.orders4 (
    id BIGINT COMMENT 'Bigint column',
    flag tinyint(1) COMMENT 'Boolean type example',
    char_col CHAR(10) NOT NULL COMMENT 'Char type example',
    date_col DATE NOT NULL COMMENT 'Date type example',
    datetime_col DATETIME COMMENT 'Datetime type example',
    decimal_col DECIMAL(18, 4) COMMENT 'Decimal type example',
    double_col DOUBLE COMMENT 'Double type example',
    float_col FLOAT COMMENT 'Float type example',
    int_col INT NOT NULL COMMENT 'Int type example',
    smallint_col SMALLINT COMMENT 'Smallint type example',
    string_col STRING COMMENT 'String type example, variable-length string',
    tinyint_col TINYINT COMMENT 'Tinyint type example',
    varchar_col VARCHAR(255) COMMENT 'Varchar type example, variable-length string',
    json_col JSON COMMENT 'Json type example, stores JSON formatted data'
) ENGINE=OLAP
DUPLICATE KEY(`id`, flag, char_col, date_col)
PARTITION BY LIST (date_col, char_col)(
    PARTITION p20230101_a VALUES IN (("2023-01-01", "A")),
    PARTITION p20230201_b VALUES IN (("2023-02-01", "B")),
    PARTITION p20230301_c VALUES IN (("2023-03-01", "C")),
    PARTITION p20230401_d VALUES IN (("2023-04-01", "D")),
    PARTITION p20230501_e VALUES IN (("2023-05-01", "E"))
)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS test2.orders5 (
    id BIGINT COMMENT 'Bigint column',
     map_col MAP<STRING, STRING> COMMENT 'Map type example, stores key-value pairs',
     array_col ARRAY<int> COMMENT 'Array type example, stores a list of values'
--     ipv4_col IPV4 ,
--     ipv6_col IPV6 COMMENT 'IPV6 type example, stores IPv6 addresses'
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "1"
);

INSERT INTO test1.orders1 (order_id,order_date,customer_name,price,product_id,order_status) VALUES
	 (1,'2024-12-05 10:28:07','xx',2.3,1,1);

INSERT INTO test1.orders2 (order_id,order_date,customer_name,price,product_id,order_status) VALUES
	 (111,'2024-12-05 10:02:31','orders2',2.3,1,1);

INSERT INTO test1.orders3 (order_id,order_date,customer_name,price,product_id,order_status) VALUES
	 (10,'2024-12-05 10:02:31','orders3',2.3,1,1),
	 (11,'2024-12-01 10:03:31','orders3-2-route',2.3,1,1),
	 (12,'2024-12-02 10:02:35','orders3',2.3,1,1);


INSERT INTO test2.orders4  VALUES
(1, TRUE, 'A', '2023-01-01', '2023-01-01 10:10:10', 1234.5678, 1.23456789, 1.2345, 123, 12, 'example string 1', 1, 'example varchar 1', '{"key1": "value1"}'),
(2, FALSE, 'B', '2023-02-01', '2023-02-02 11:11:11', 9876.5432, 9.87654321, 9.8765, 456, 34, 'example string 2', 2, 'example varchar 2', '{"key2": "value2"}'),
(3, TRUE, 'C', '2023-03-01', '2023-03-03 12:12:12', 5678.1234, 5.67812345, 5.6789, 789, 56, 'example string 3', 3, 'example varchar 3',  '{"key3": "value3"}'),
(4, FALSE, 'D', '2023-04-01', '2023-04-04 13:13:13', 4321.8765, 4.32187654, 4.3211, 101, 78, 'example string 4', 4, 'example varchar 4',  '{"key4": "value4"}'),
(5, TRUE, 'E', '2023-05-01', '2023-05-05 14:14:14', 8765.4321, 8.76543210, 8.7654, 202, 90, 'example string 5', 5, 'example varchar 5',  '{"key5": "value5"}');


-- TODO: Doris flink connector read IPV4,IPV6 failed
--INSERT INTO test2.orders5  VALUES
--(1, {'a': '100', 'b': '200'},[6,7,8],'127.0.0.1', '2001:16a0:2:200a::2');

INSERT INTO test2.orders5  VALUES
(1, {'a': '100', 'b': '200'},[6,7,8]);
