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

suite("test_mysql_jdbc_create_table", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return;
    }

    String user = "test_jdbc_user";
    String pwd = '123456';
    String mysql_port = context.config.otherConfigs.get("mysql_57_port");
    String catalog_name = "mysql_create_table_test";
    String ex_db_name = "doris_test";

    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties(
        "type"="jdbc",
        "jdbc_user"="${user}",
        "jdbc_password"="${pwd}",
        "jdbc_url"="jdbc:mysql://${externalEnvIp}:${mysql_port}/${ex_db_name}?permitMysqlScheme",
        "driver_url"="${driver_url}",
        "driver_class"="com.mysql.cj.jdbc.Driver"
    )"""

    // ===== Test 1: Basic types + NOT NULL =====
    logger.info("=== Test 1: CREATE TABLE with basic types ===")
    sql """drop table if exists ${catalog_name}.${ex_db_name}.ex_create_t1"""
    sql """CREATE TABLE ${catalog_name}.${ex_db_name}.ex_create_t1 (
        id INT,
        name VARCHAR(100),
        price DECIMAL(10,2),
        amount INT NOT NULL
    )"""
    order_qt_t1_desc """DESC ${catalog_name}.${ex_db_name}.ex_create_t1"""

    // Insert and query
    sql """INSERT INTO ${catalog_name}.${ex_db_name}.ex_create_t1 VALUES
        (1, 'apple', 10.50, 100),
        (2, 'banana', 5.00, 200)"""
    order_qt_t1_select """SELECT * FROM ${catalog_name}.${ex_db_name}.ex_create_t1 ORDER BY id"""
    sql """drop table if exists ${catalog_name}.${ex_db_name}.ex_create_t1"""

    // ===== Test 2: All type mappings =====
    logger.info("=== Test 2: All type mappings ===")
    sql """drop table if exists ${catalog_name}.${ex_db_name}.ex_create_types"""
    sql """CREATE TABLE ${catalog_name}.${ex_db_name}.ex_create_types (
        c_tinyint TINYINT,
        c_smallint SMALLINT,
        c_int INT,
        c_bigint BIGINT,
        c_largeint LARGEINT,
        c_float FLOAT,
        c_double DOUBLE,
        c_decimal DECIMAL(15,4),
        c_char CHAR(10),
        c_varchar VARCHAR(200),
        c_text STRING,
        c_date DATE,
        c_datetime DATETIME,
        c_datetime6 DATETIME(3),
        c_boolean BOOLEAN,
        c_json JSONB
    )"""
    order_qt_types_desc """DESC ${catalog_name}.${ex_db_name}.ex_create_types"""

    // Insert and verify all types, including LARGEINT with value > BIGINT max
    sql """INSERT INTO ${catalog_name}.${ex_db_name}.ex_create_types VALUES (
        127, 32767, 2147483647, 9223372036854775807,
        18446744073709551615,
        1.5, 2.5, 123.4567,
        'abc', 'hello world', 'some text',
        '2024-06-15', '2024-06-15 12:30:00', '2024-06-15 12:30:00.123',
        true, '{"key": "value"}'
    )"""
    order_qt_types_select """SELECT * FROM ${catalog_name}.${ex_db_name}.ex_create_types"""
    sql """drop table if exists ${catalog_name}.${ex_db_name}.ex_create_types"""

    // ===== Test 3: Unsupported types should error =====
    logger.info("=== Test 3: Unsupported types ===")
    test {
        sql """CREATE TABLE ${catalog_name}.${ex_db_name}.ex_create_unsupported (
            c_hll HLL
        )"""
        exception "UNSUPPORTED"
    }
    test {
        sql """CREATE TABLE ${catalog_name}.${ex_db_name}.ex_create_unsupported (
            c_bitmap BITMAP
        )"""
        exception "UNSUPPORTED"
    }
    test {
        sql """CREATE TABLE ${catalog_name}.${ex_db_name}.ex_create_unsupported (
            c_quantile QUANTILE_STATE
        )"""
        exception "UNSUPPORTED"
    }

    // Cleanup catalog
    sql """drop catalog if exists ${catalog_name}"""
}
