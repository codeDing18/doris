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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.jdbc.client.JdbcConnectorClient;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for CREATE TABLE SQL generation in {@link JdbcConnectorMetadata}.
 */
class JdbcConnectorMetadataCreateTableTest {

    private static final String LAST_SQL_KEY = "lastSql";

    /**
     * Creates a JdbcConnectorMetadata backed by a client that captures the last
     * executed SQL string.
     */
    private static JdbcConnectorMetadata createMetadataWithCapture(
            Map<String, String> captured) {
        JdbcConnectorClient client = new JdbcMySQLConnectorClient(
                "test_catalog", JdbcDbType.MYSQL,
                "jdbc:mysql://localhost:3306/testdb", false,
                Collections.emptyMap(), Collections.emptyMap(),
                false, false) {
            @Override
            public void executeStmt(String origStmt) {
                captured.put(LAST_SQL_KEY, origStmt);
            }
        };
        return new JdbcConnectorMetadata(client, Collections.emptyMap());
    }

    private static ConnectorSession testSession() {
        return new ConnectorSession() {
            @Override
            public String getQueryId() { return "test"; }
            @Override
            public String getUser() { return "root"; }
            @Override
            public String getTimeZone() { return "UTC"; }
            @Override
            public String getLocale() { return "en_US"; }
            @Override
            public long getCatalogId() { return 0L; }
            @Override
            public String getCatalogName() { return "test"; }
            @Override
            public <T> T getProperty(String name, Class<T> type) { return null; }
            @Override
            public Map<String, String> getCatalogProperties() {
                return Collections.emptyMap();
            }
            @Override
            public Map<String, String> getSessionProperties() {
                return Collections.emptyMap();
            }
        };
    }

    @Test
    void testCreateTableSimpleTypes() {
        java.util.HashMap<String, String> captured = new java.util.HashMap<>();
        JdbcConnectorMetadata metadata = createMetadataWithCapture(captured);

        List<ConnectorColumn> columns = new ArrayList<>();
        columns.add(new ConnectorColumn("id", ConnectorType.of("INT"), null, true, null));
        columns.add(new ConnectorColumn("name", new ConnectorType("VARCHAR", 100, -1), null, true, null));

        ConnectorTableSchema schema = new ConnectorTableSchema(
                "test_tbl", columns, "JDBC", Collections.emptyMap());

        metadata.createTable(testSession(), schema, Collections.emptyMap());

        String sql = captured.get(LAST_SQL_KEY);
        Assertions.assertNotNull(sql, "executeStmt should have been called");
        Assertions.assertTrue(sql.contains("CREATE TABLE"),
                "Should start with CREATE TABLE. SQL: " + sql);
        Assertions.assertTrue(sql.contains("`test_tbl`"),
                "Should quote table name. SQL: " + sql);
        Assertions.assertTrue(sql.contains("`id` INT"),
                "Should have INT column. SQL: " + sql);
        Assertions.assertTrue(sql.contains("`name` VARCHAR(100)"),
                "Should have VARCHAR(100) column. SQL: " + sql);
    }

    @Test
    void testCreateTableDecimal() {
        java.util.HashMap<String, String> captured = new java.util.HashMap<>();
        JdbcConnectorMetadata metadata = createMetadataWithCapture(captured);

        List<ConnectorColumn> columns = new ArrayList<>();
        columns.add(new ConnectorColumn("price",
                ConnectorType.of("DECIMAL", 10, 2), null, true, null));

        ConnectorTableSchema schema = new ConnectorTableSchema(
                "orders", columns, "JDBC", Collections.emptyMap());

        metadata.createTable(testSession(), schema, Collections.emptyMap());

        String sql = captured.get(LAST_SQL_KEY);
        Assertions.assertNotNull(sql);
        Assertions.assertTrue(sql.contains("`price` DECIMAL(10,2)"),
                "Should have DECIMAL(10,2). SQL: " + sql);
    }

    @Test
    void testCreateTableNotNull() {
        java.util.HashMap<String, String> captured = new java.util.HashMap<>();
        JdbcConnectorMetadata metadata = createMetadataWithCapture(captured);

        List<ConnectorColumn> columns = new ArrayList<>();
        columns.add(new ConnectorColumn("id", ConnectorType.of("INT"), null, false, null));

        ConnectorTableSchema schema = new ConnectorTableSchema(
                "t", columns, "JDBC", Collections.emptyMap());

        metadata.createTable(testSession(), schema, Collections.emptyMap());

        String sql = captured.get(LAST_SQL_KEY);
        Assertions.assertNotNull(sql);
        Assertions.assertTrue(sql.contains("NOT NULL"),
                "NOT NULL should be present. SQL: " + sql);
    }

    @Test
    void testCreateTableMultipleColumns() {
        java.util.HashMap<String, String> captured = new java.util.HashMap<>();
        JdbcConnectorMetadata metadata = createMetadataWithCapture(captured);

        List<ConnectorColumn> columns = new ArrayList<>();
        columns.add(new ConnectorColumn("a", ConnectorType.of("INT"), null, true, null));
        columns.add(new ConnectorColumn("b", ConnectorType.of("BIGINT"), null, true, null));
        columns.add(new ConnectorColumn("c", ConnectorType.of("VARCHAR", 255, -1), null, false, null));

        ConnectorTableSchema schema = new ConnectorTableSchema(
                "multi", columns, "JDBC", Collections.emptyMap());

        metadata.createTable(testSession(), schema, Collections.emptyMap());

        String sql = captured.get(LAST_SQL_KEY);
        Assertions.assertNotNull(sql);
        Assertions.assertTrue(sql.contains("`a` INT"), "Column a. SQL: " + sql);
        Assertions.assertTrue(sql.contains("`b` BIGINT"), "Column b. SQL: " + sql);
        Assertions.assertTrue(sql.contains("`c` VARCHAR(255) NOT NULL"),
                "Column c with NOT NULL. SQL: " + sql);
    }

    @Test
    void testToSqlType() throws Exception {
        java.lang.reflect.Method method = JdbcConnectorMetadata.class
                .getDeclaredMethod("toSqlType", ConnectorType.class);
        method.setAccessible(true);

        Assertions.assertEquals("INT", method.invoke(null, ConnectorType.of("INT")));
        Assertions.assertEquals("VARCHAR(100)",
                method.invoke(null, new ConnectorType("VARCHAR", 100, -1)));
        Assertions.assertEquals("DECIMAL(10,2)",
                method.invoke(null, ConnectorType.of("DECIMAL", 10, 2)));
        Assertions.assertEquals("CHAR(10)",
                method.invoke(null, new ConnectorType("CHAR", 10, -1)));
    }
}
