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

package org.apache.doris.connector.api.pushdown;

import org.apache.doris.connector.api.ConnectorType;

import java.io.Serializable;
import java.util.Objects;

/**
 * Describes a simple aggregate function to push down to a connector.
 *
 * <p>Represents a function call of the form {@code FN([DISTINCT ]column) AS alias}.
 * For {@code COUNT(*)}, use {@code columnName = "*"}.
 *
 * <p>{@code columnType} carries the column's {@link ConnectorType} so the connector
 * can decide pushability and SQL rewriting by type (e.g. {@code AVG} on integer needs
 * {@code *1.0}, on decimal needs {@code CAST}; {@code MIN}/{@code MAX} on textual types
 * may be rejected). It is {@code null} for {@code COUNT(*)}.
 */
public final class ConnectorAggregate implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String functionName;  // nereids function name, lowercase, e.g. "sum", "count"
    private final String columnName;    // column name, or "*" for count(*)
    private final String alias;         // output alias
    private final boolean distinct;     // function-level DISTINCT
    private final ConnectorType columnType;  // null for count(*)

    public ConnectorAggregate(String functionName, String columnName, String alias,
            boolean distinct, ConnectorType columnType) {
        this.functionName = Objects.requireNonNull(functionName, "functionName");
        this.columnName = Objects.requireNonNull(columnName, "columnName");
        this.alias = Objects.requireNonNull(alias, "alias");
        this.distinct = distinct;
        this.columnType = columnType;
    }

    public String getFunctionName() {
        return functionName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getAlias() {
        return alias;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public ConnectorType getColumnType() {
        return columnType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorAggregate)) {
            return false;
        }
        ConnectorAggregate that = (ConnectorAggregate) o;
        return distinct == that.distinct
                && functionName.equals(that.functionName)
                && columnName.equals(that.columnName)
                && alias.equals(that.alias)
                && Objects.equals(columnType, that.columnType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionName, columnName, alias, distinct, columnType);
    }

    @Override
    public String toString() {
        return functionName + "(" + (distinct ? "DISTINCT " : "") + columnName + ") AS " + alias;
    }
}
