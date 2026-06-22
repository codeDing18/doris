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

// Regression tests for JDBC simple aggregate pushdown (SUM/COUNT/AVG/MIN/MAX).
// Requires a MySQL catalog with the doris_test database (ex_tb0 has columns: id, name).

suite("test_jdbc_simple_agg_pushdown", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return;
    }

    String mysql_port = context.config.otherConfigs.get("mysql_57_port");
    String catalog_name = "jdbc_agg_pushdown_test"
    String ex_db_name = "doris_test";
    String ex_tb0 = "ex_tb0";

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

    // Enable JDBC simple aggregate pushdown.
    sql "set enable_jdbc_pushdown_aggregate = true;"

    // ---- Single function tests ----
    order_qt_sum  """ select sum(id) from ${ex_tb0} """
    order_qt_count_star """ select count(*) from ${ex_tb0} """
    order_qt_count_col """ select count(id) from ${ex_tb0} """
    order_qt_min  """ select min(id) from ${ex_tb0} """
    order_qt_max  """ select max(id) from ${ex_tb0} """
    order_qt_avg  """ select avg(id) from ${ex_tb0} """

    // ---- Mixed functions ----
    order_qt_mixed """ select sum(id), count(*), avg(id), min(id), max(id) from ${ex_tb0} """

    // ---- DISTINCT ----
    order_qt_count_distinct """ select count(distinct id) from ${ex_tb0} """

    // ---- With WHERE ----
    order_qt_sum_where """ select sum(id) from ${ex_tb0} where id > 0 """

    // ---- EXPLAIN verifies pushdown SQL ----
    explain {
        sql("select sum(id) from ${ex_tb0}")
        contains("SUM(")
    }
    explain {
        sql("select count(*) from ${ex_tb0}")
        contains("COUNT(*)")
    }
    explain {
        sql("select count(distinct id) from ${ex_tb0}")
        contains("COUNT(DISTINCT")
    }

    // ---- Switch disabled: no pushdown ----
    sql "set enable_jdbc_pushdown_aggregate = false;"
    explain {
        sql("select sum(id) from ${ex_tb0}")
        notContains("SUM(`id`)")
    }

    sql """drop catalog if exists ${catalog_name}"""
}
