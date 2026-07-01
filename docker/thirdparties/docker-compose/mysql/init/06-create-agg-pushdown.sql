-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Dedicated table for JDBC aggregate pushdown regression tests.
-- Covers all column types that interact with type-aware pushdown:
--   - integer family (int/bigint/smallint): avg must render as AVG((col * 1.0))
--   - decimal/double/float: avg renders as plain AVG(col)
--   - varchar/char/text: min/max must NOT be pushed down (MySQL case-insensitive collation)

create table doris_test.agg_pushdown_test (
    `id` int auto_increment primary key,
    `c_int` int,
    `c_bigint` bigint,
    `c_smallint` smallint,
    `c_decimal` decimal(12, 4),
    `c_double` double,
    `c_float` float,
    `c_varchar` varchar(32),
    `c_char` char(5),
    `c_text` text
) engine=innodb charset=utf8;

-- Data chosen so that aggregates have non-trivial results:
--   - avg(c_int) of [1,2,4] over non-null rows = 2.3333... (needs fraction, tests *1.0)
--   - row 3 has NULLs in c_int / c_varchar (tests count(col) vs count(*))
--   - decimal/double values have fractional parts (tests precision preservation)
--   - textual values differ in case (tests that min/max are NOT pushed down for textual)
insert into doris_test.agg_pushdown_test
    (c_int, c_bigint, c_smallint, c_decimal, c_double, c_float, c_varchar, c_char, c_text)
values
    (1,    100,  10, 10.5000,  1.5,  1.25, 'banana', 'apple', 'hello'),
    (2,    200,  20, 20.5000,  2.5,  2.25, 'APPLE',  'berry', 'world'),
    (4,    400,  40, 40.5000,  4.5,  4.25, 'Cherry', 'apple', 'hello'),
    (null, null, null, null, null, null, null,     null,    null);
