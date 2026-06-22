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

import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorAggregate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Opaque table handle carrying the remote database/table coordinates
 * and optional pushdown aggregates.
 */
public class JdbcTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    private final String remoteDbName;
    private final String remoteTableName;
    private final List<ConnectorAggregate> pushDownAggregates;

    public JdbcTableHandle(String remoteDbName, String remoteTableName) {
        this(remoteDbName, remoteTableName, Collections.emptyList());
    }

    public JdbcTableHandle(String remoteDbName, String remoteTableName,
            List<ConnectorAggregate> pushDownAggregates) {
        this.remoteDbName = remoteDbName;
        this.remoteTableName = remoteTableName;
        this.pushDownAggregates = new ArrayList<>(pushDownAggregates);
    }

    public String getRemoteDbName() {
        return remoteDbName;
    }

    public String getRemoteTableName() {
        return remoteTableName;
    }

    public List<ConnectorAggregate> getPushDownAggregates() {
        return pushDownAggregates;
    }

    public boolean hasPushDownAggregates() {
        return !pushDownAggregates.isEmpty();
    }

    public JdbcTableHandle withPushDownAggregates(List<ConnectorAggregate> aggregates) {
        return new JdbcTableHandle(remoteDbName, remoteTableName, aggregates);
    }

    @Override
    public String toString() {
        return "JdbcTableHandle{" + remoteDbName + "." + remoteTableName
                + (hasPushDownAggregates() ? ", aggs=" + pushDownAggregates : "") + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JdbcTableHandle)) {
            return false;
        }
        JdbcTableHandle that = (JdbcTableHandle) o;
        return Objects.equals(remoteDbName, that.remoteDbName)
                && Objects.equals(remoteTableName, that.remoteTableName)
                && Objects.equals(pushDownAggregates, that.pushDownAggregates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteDbName, remoteTableName, pushDownAggregates);
    }
}
