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

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import java.util.Objects;

/**
 * Result of applying aggregates to a table handle.
 *
 * @param <T> the table handle type
 */
public final class AggregateApplicationResult<T extends ConnectorTableHandle> {

    private final T handle;

    public AggregateApplicationResult(T handle) {
        this.handle = Objects.requireNonNull(handle, "handle");
    }

    public T getHandle() {
        return handle;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AggregateApplicationResult)) {
            return false;
        }
        AggregateApplicationResult<?> that = (AggregateApplicationResult<?>) o;
        return handle.equals(that.handle);
    }

    @Override
    public int hashCode() {
        return Objects.hash(handle);
    }

    @Override
    public String toString() {
        return "AggregateApplicationResult{handle=" + handle + "}";
    }
}
