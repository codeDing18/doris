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
    String ex_tb_name = "ex_create_table_test";

    // Ensure test table does not exist before starting
    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties(
        "type"="jdbc",
        "jdbc_user"="${user}",
        "jdbc_password"="${pwd}",
        "jdbc_url"="jdbc:mysql://${externalEnvIp}:${mysql_port}/${ex_db_name}?permitMysqlScheme",
        "driver_url"="${driver_url}",
        "driver_class"="com.mysql.cj.jdbc.Driver"
    )"""

    // Ensure clean state
    try {
        sql """drop table if exists ${catalog_name}.${ex_db_name}.${ex_tb_name}"""
    } catch (Exception e) {
        // table may not exist, ignore
    }

    // ===== Test 1: Create table with basic types =====
    logger.info("=== Test 1: CREATE TABLE with basic types ===")
    sql """CREATE TABLE ${catalog_name}.${ex_db_name}.${ex_tb_name} (
        id INT,
        name VARCHAR(100),
        price DECIMAL(10,2),
        amount INT NOT NULL
    )"""
    order_qt_desc """DESC ${catalog_name}.${ex_db_name}.${ex_tb_name}"""

    // ===== Test 2: Insert and query =====
    logger.info("=== Test 2: INSERT and SELECT ===")
    sql """INSERT INTO ${catalog_name}.${ex_db_name}.${ex_tb_name} VALUES
        (1, 'apple', 10.50, 100),
        (2, 'banana', 5.00, 200)"""
    order_qt_select """SELECT * FROM ${catalog_name}.${ex_db_name}.${ex_tb_name} ORDER BY id"""

    // ===== Test 3: Create table with different column types =====
    logger.info("=== Test 3: CREATE TABLE with various types ===")
    sql """drop table if exists ${catalog_name}.${ex_db_name}.${ex_tb_name}_types"""
    sql """CREATE TABLE ${catalog_name}.${ex_db_name}.${ex_tb_name}_types (
        c_tinyint TINYINT,
        c_smallint SMALLINT,
        c_int INT,
        c_bigint BIGINT,
        c_float FLOAT,
        c_double DOUBLE,
        c_decimal DECIMAL(15,4),
        c_varchar VARCHAR(200),
        c_char CHAR(10),
        c_date DATE,
        c_datetime DATETIME,
        c_text STRING,
        c_boolean BOOLEAN
    )"""
    order_qt_desc_types """DESC ${catalog_name}.${ex_db_name}.${ex_tb_name}_types"""

    // ===== Test 4: Insert into typed table =====
    logger.info("=== Test 4: INSERT into typed table ===")
    sql """INSERT INTO ${catalog_name}.${ex_db_name}.${ex_tb_name}_types VALUES
        (1, 2, 3, 4, 1.5, 2.5, 123.4567, 'hello', 'abc', '2024-01-01', '2024-01-01 12:00:00', 'text', true)"""
    order_qt_select_types """SELECT * FROM ${catalog_name}.${ex_db_name}.${ex_tb_name}_types"""

    // ===== Test 5: DROP TABLE =====
    logger.info("=== Test 5: DROP TABLE ===")
    sql """drop table if exists ${catalog_name}.${ex_db_name}.${ex_tb_name}"""
    sql """drop table if exists ${catalog_name}.${ex_db_name}.${ex_tb_name}_types"""

    // Cleanup
    sql """drop catalog if exists ${catalog_name}"""
}
