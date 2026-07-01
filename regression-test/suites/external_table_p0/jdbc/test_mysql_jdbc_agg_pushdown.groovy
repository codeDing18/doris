// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Regression tests for MySQL JDBC aggregate pushdown (SUM/COUNT/AVG/MIN/MAX).
// Covers type-aware correctness:
//   - avg(integer family) -> AVG((col * 1.0))  (Doris returns double, MySQL would truncate)
//   - avg(decimal/double/float) -> AVG(col)    (MySQL native precision is sufficient)
//   - min/max(varchar/char/text) -> NOT pushed down  (MySQL case-insensitive collation)
//
// Table doris_test.agg_pushdown_test (from docker mysql init 06-create-agg-pushdown.sql):
//   c_int, c_bigint, c_smallint (integer family)
//   c_decimal decimal(12,4), c_double, c_float
//   c_varchar, c_char, c_text (textual — min/max must not push down)
//   3 non-null rows + 1 all-null row (tests count(col) vs count(*))

suite("test_mysql_jdbc_agg_pushdown", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return;
    }

    String mysql_port = context.config.otherConfigs.get("mysql_57_port");
    String catalog_name = "mysql_agg_pushdown_test"
    String ex_db_name = "doris_test";
    String tbl = "agg_pushdown_test";

    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties(
        "type"="jdbc",
        "user"="root",
        "password"="123456",
        "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
        "driver_url" = "${driver_url}",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
    );"""

    sql """use ${catalog_name}.${ex_db_name}"""

    sql "set enable_jdbc_pushdown_aggregate = true;"

    // ========================================================================
    // Part 1: Basic aggregates on integer column (c_int)
    //   avg(c_int) over [1,2,4] = 2.3333... — exercises the *1.0 rewrite.
    // ========================================================================
    order_qt_sum_int       """ select sum(c_int) from ${tbl} """
    order_qt_count_star    """ select count(*) from ${tbl} """
    order_qt_count_int     """ select count(c_int) from ${tbl} """
    order_qt_min_int       """ select min(c_int) from ${tbl} """
    order_qt_max_int       """ select max(c_int) from ${tbl} """
    order_qt_avg_int       """ select avg(c_int) from ${tbl} """

    // Multiple aggregates in one query.
    order_qt_mixed_int     """ select sum(c_int), count(*), avg(c_int), min(c_int), max(c_int) from ${tbl} """

    // count(*) vs count(col): row 4 has NULL, so count(*) = 4 but count(c_int) = 3.
    order_qt_count_null    """ select count(*), count(c_int), count(c_bigint) from ${tbl} """

    // ========================================================================
    // Part 2: avg type-aware rendering across the integer family.
    //   Each must push down with the *1.0 promotion.
    // ========================================================================
    explain {
        sql("select avg(c_int) from ${tbl}")
        contains("* 1.0)")
    }
    explain {
        sql("select avg(c_bigint) from ${tbl}")
        contains("* 1.0)")
    }
    explain {
        sql("select avg(c_smallint) from ${tbl}")
        contains("* 1.0)")
    }

    // Result correctness for integer-family avg.
    order_qt_avg_bigint    """ select avg(c_bigint) from ${tbl} """
    order_qt_avg_smallint  """ select avg(c_smallint) from ${tbl} """

    // ========================================================================
    // Part 3: avg on decimal/double/float — plain AVG (no *1.0, no CAST).
    //   avg(c_decimal) over [10.5, 20.5, 40.5] = 23.8333...
    // ========================================================================
    explain {
        sql("select avg(c_decimal) from ${tbl}")
        contains("AVG(`c_decimal`)")
        notContains("* 1.0")
        notContains("CAST")
    }
    explain {
        sql("select avg(c_double) from ${tbl}")
        contains("AVG(`c_double`)")
        notContains("* 1.0")
        notContains("CAST")
    }
    explain {
        sql("select avg(c_float) from ${tbl}")
        contains("AVG(`c_float`)")
        notContains("* 1.0")
        notContains("CAST")
    }

    order_qt_avg_decimal   """ select avg(c_decimal) from ${tbl} """
    order_qt_avg_double    """ select avg(c_double) from ${tbl} """
    order_qt_avg_float     """ select avg(c_float) from ${tbl} """

    // ========================================================================
    // Part 4: sum / min / max on numeric types (all pushed down).
    // ========================================================================
    order_qt_sum_bigint    """ select sum(c_bigint) from ${tbl} """
    order_qt_sum_decimal   """ select sum(c_decimal) from ${tbl} """
    order_qt_min_decimal   """ select min(c_decimal) from ${tbl} """
    order_qt_max_decimal   """ select max(c_decimal) from ${tbl} """
    order_qt_min_double    """ select min(c_double) from ${tbl} """
    order_qt_max_double    """ select max(c_double) from ${tbl} """
    order_qt_min_bigint    """ select min(c_bigint) from ${tbl} """
    order_qt_max_bigint    """ select max(c_bigint) from ${tbl} """

    explain {
        sql("select sum(c_int) from ${tbl}")
        contains("SUM(")
    }
    explain {
        sql("select min(c_int) from ${tbl}")
        contains("MIN(")
    }
    explain {
        sql("select max(c_int) from ${tbl}")
        contains("MAX(")
    }

    // ========================================================================
    // Part 5: min/max on textual types NOT pushed down (MySQL collation).
    //   Data has mixed case ('banana', 'APPLE', 'Cherry'); MySQL case-insensitive
    //   sort would give a different result than Doris, so these must stay local.
    //   We verify correctness via order_qt (local computation).
    // ========================================================================
    order_qt_min_varchar   """ select min(c_varchar) from ${tbl} """
    order_qt_max_varchar   """ select max(c_varchar) from ${tbl} """
    order_qt_min_char      """ select min(c_char) from ${tbl} """
    order_qt_max_char      """ select max(c_char) from ${tbl} """
    order_qt_min_text      """ select min(c_text) from ${tbl} """
    order_qt_max_text      """ select max(c_text) from ${tbl} """

    // ========================================================================
    // Part 6: count on textual IS pushed down (count is collation-independent).
    // ========================================================================
    explain {
        sql("select count(c_varchar) from ${tbl}")
        contains("COUNT(")
    }
    order_qt_count_varchar """ select count(c_varchar) from ${tbl} """

    // ========================================================================
    // Part 7: DISTINCT is not pushed down (rule bails out).
    // ========================================================================
    order_qt_count_distinct """ select count(distinct c_int) from ${tbl} """

    // ========================================================================
    // Part 8: Switch disabled -> no pushdown.
    //   The *1.0 signature must be absent; results must stay correct (local agg).
    // ========================================================================
    sql "set enable_jdbc_pushdown_aggregate = false;"
    explain {
        sql("select avg(c_int) from ${tbl}")
        notContains("* 1.0)")
    }
    order_qt_disabled_avg_int     """ select avg(c_int) from ${tbl} """
    order_qt_disabled_sum_decimal """ select sum(c_decimal) from ${tbl} """

    // ========================================================================
    // Part 9: re-enable; results must be identical to local computation.
    // ========================================================================
    sql "set enable_jdbc_pushdown_aggregate = true;"
    order_qt_consistency_avg     """ select avg(c_int) from ${tbl} """
    order_qt_consistency_sum     """ select sum(c_decimal) from ${tbl} """
    order_qt_consistency_mix     """ select sum(c_decimal), avg(c_int), max(c_double) from ${tbl} """

    sql """drop catalog if exists ${catalog_name}"""
}
