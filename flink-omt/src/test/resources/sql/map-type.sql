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

CREATE TABLE IF NOT EXISTS map.map_test (
  order_id int,
  array0 ARRAY<varchar(225)>,
  array1 ARRAY<ARRAY<int>>,
  array7  ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<boolean>>>>>>>,
  map1 MAP<int, boolean>
)
DISTRIBUTED BY HASH(order_id)
PROPERTIES (
    "replication_num" = "1"
);

INSERT INTO map.map_test values(1, ['xxx','yyy'],[[1,2], [3,4]] , [[[[[[[true,false]]]]]]],map(5, true, 7, false));
