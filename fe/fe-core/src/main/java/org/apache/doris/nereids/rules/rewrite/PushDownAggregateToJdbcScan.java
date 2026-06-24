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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.connector.api.pushdown.ConnectorAggregate;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Pushes simple aggregate functions (SUM/COUNT/AVG/MIN/MAX) down to JDBC catalogs
 * (currently MySQL only) for global aggregates (no GROUP BY).
 *
 * <p>The rule matches {@code LogicalAggregate(LogicalFileScan)} or
 * {@code LogicalAggregate(LogicalProject(LogicalFileScan))} on a JDBC catalog. It
 * collects supported aggregate functions into {@link ConnectorAggregate} objects,
 * attaches them to the scan via {@code pushdownJdbcSimpleAggregates}, and replaces
 * the aggregate (and optional project) with the scan alone. The scan's output is
 * the aggregate's output so that upper operators (having/order/limit) keep valid.
 *
 * <p>Conservatively bails out if any function or argument is unsupported.
 */
public class PushDownAggregateToJdbcScan extends OneRewriteRuleFactory {

    private static final Logger LOG = LogManager.getLogger(PushDownAggregateToJdbcScan.class);

    @Override
    public Rule build() {
        return logicalAggregate().then(this::tryPushDown).toRule(RuleType.JDBC_AGGREGATE_PUSHDOWN);
    }

    /**
     * Tries to push down the aggregate to a JDBC file scan. Returns null (no rewrite)
     * when the shape is not {@code agg(scan)} or {@code agg(project(scan))} on a JDBC
     * catalog, the session variable is disabled, or any aggregate is unsupported.
     */
    private Plan tryPushDown(LogicalAggregate<Plan> aggregate) {
        if (!enableJdbcPushDownAggregate()) {
            LOG.info("JDBC_AGG_PUSHDOWN: skip, enable_jdbc_pushdown_aggregate=false");
            return null;
        }
        if (!aggregate.getGroupByExpressions().isEmpty()) {
            LOG.info("JDBC_AGG_PUSHDOWN: skip, aggregate has GROUP BY");
            return null;
        }
        if (!aggregate.getDistinctArguments().isEmpty()) {
            LOG.info("JDBC_AGG_PUSHDOWN: skip, aggregate has DISTINCT arguments");
            return null;
        }
        LOG.info("JDBC_AGG_PUSHDOWN: matching aggregate with output={}, child class={}",
                aggregate.getOutput(), aggregate.child(0).getClass().getSimpleName());

        // Unwrap optional LogicalProject to reach the file scan.
        Plan child = aggregate.child(0);
        LogicalProject<? extends Plan> project = null;
        if (child instanceof LogicalProject) {
            project = (LogicalProject<? extends Plan>) child;
            if (project.child(0) instanceof LogicalFileScan) {
                child = project.child(0);
            } else {
                LOG.info("JDBC_AGG_PUSHDOWN: skip, project child is not LogicalFileScan but {}",
                        project.child(0).getClass().getSimpleName());
                return null;
            }
        }
        if (!(child instanceof LogicalFileScan)) {
            LOG.info("JDBC_AGG_PUSHDOWN: skip, aggregate child is not LogicalFileScan but {}",
                    aggregate.child(0).getClass().getSimpleName());
            return null;
        }
        LogicalFileScan fileScan = (LogicalFileScan) child;
        if (!isJdbcCatalog(fileScan)) {
            LOG.info("JDBC_AGG_PUSHDOWN: skip, not a supported JDBC catalog (table={})",
                    fileScan.getTable() == null ? "null" : fileScan.getTable().getName());
            return null;
        }

        Plan result = pushdownAggregate(aggregate, project, fileScan);
        if (result == null) {
            LOG.info("JDBC_AGG_PUSHDOWN: skip, some aggregate function or argument is unsupported");
        } else {
            LOG.info("JDBC_AGG_PUSHDOWN: pushed down aggregates to JDBC scan for table {}",
                    fileScan.getTable() == null ? "null" : fileScan.getTable().getName());
        }
        return result;
    }

    @VisibleForTesting
    protected boolean enableJdbcPushDownAggregate() {
        ConnectContext context = ConnectContext.get();
        return context != null && context.getSessionVariable().isEnableJdbcPushdownAggregate();
    }

    @VisibleForTesting
    protected boolean isJdbcCatalog(LogicalFileScan fileScan) {
        if (!(fileScan.getTable() instanceof PluginDrivenExternalTable)) {
            LOG.info("JDBC_AGG_PUSHDOWN: table is not PluginDrivenExternalTable but {}",
                    fileScan.getTable() == null ? "null" : fileScan.getTable().getClass().getSimpleName());
            return false;
        }
        PluginDrivenExternalTable table = (PluginDrivenExternalTable) fileScan.getTable();
        if (!(table.getCatalog() instanceof PluginDrivenExternalCatalog)) {
            LOG.info("JDBC_AGG_PUSHDOWN: catalog is not PluginDrivenExternalCatalog but {}",
                    table.getCatalog() == null ? "null" : table.getCatalog().getClass().getSimpleName());
            return false;
        }
        PluginDrivenExternalCatalog catalog = (PluginDrivenExternalCatalog) table.getCatalog();
        if (!"jdbc".equalsIgnoreCase(catalog.getType())) {
            LOG.info("JDBC_AGG_PUSHDOWN: catalog type is '{}', not 'jdbc'", catalog.getType());
            return false;
        }
        String jdbcUrl = catalog.getCatalogProperty().getOrDefault("jdbc_url", "");
        // Currently only MySQL is supported; extend to other JDBC types later.
        boolean isMysql = jdbcUrl.toLowerCase().startsWith("jdbc:mysql:");
        if (!isMysql) {
            LOG.info("JDBC_AGG_PUSHDOWN: jdbc_url '{}' does not start with 'jdbc:mysql:'", jdbcUrl);
        }
        return isMysql;
    }

    /**
     * Returns true if the aggregate function is supported for JDBC pushdown:
     * SUM/COUNT/AVG/MIN/MAX with a single SlotReference argument, or COUNT(*).
     */
    private boolean isSupported(AggregateFunction func) {
        if (func instanceof Sum || func instanceof Avg || func instanceof Min || func instanceof Max) {
            return func.arity() == 1 && func.child(0) instanceof SlotReference;
        }
        if (func instanceof Count) {
            Count count = (Count) func;
            if (count.isCountStar()) {
                return true;
            }
            return count.arity() == 1 && count.child(0) instanceof SlotReference;
        }
        return false;
    }

    /**
     * Builds the new scan carrying the aggregates. Returns the original aggregate if
     * any aggregate function or argument is unsupported.
     */
    private Plan pushdownAggregate(LogicalAggregate<? extends Plan> aggregate,
            LogicalProject<? extends Plan> project, LogicalFileScan fileScan) {
        // Derive (aggFunction, outputSlot) pairs from the output expressions.
        // Each output expression must wrap exactly one aggregate function (no GROUP BY).
        List<AggregateFunction> aggregateFunctions = new ArrayList<>();
        List<Slot> aggOutputSlots = new ArrayList<>();
        for (NamedExpression outputExpr : aggregate.getOutputExpressions()) {
            Set<AggregateFunction> funcs = outputExpr.collect(AggregateFunction.class::isInstance);
            if (funcs.size() != 1) {
                LOG.info("JDBC_AGG_PUSHDOWN: output expression '{}' has {} aggregate functions (need exactly 1)",
                        outputExpr, funcs.size());
                return null;
            }
            AggregateFunction aggFunc = funcs.iterator().next();
            if (!isSupported(aggFunc)) {
                LOG.info("JDBC_AGG_PUSHDOWN: aggregate function '{}' is not supported for pushdown", aggFunc);
                return null;
            }
            aggregateFunctions.add(aggFunc);
            aggOutputSlots.add(outputExpr.toSlot());
        }

        List<ConnectorAggregate> aggregates = new ArrayList<>();
        for (int index = 0; index < aggregateFunctions.size(); index++) {
            AggregateFunction aggFunc = aggregateFunctions.get(index);
            String functionName = aggFunc.getName().toLowerCase();
            String sqlFunctionName;
            String columnName;
            boolean distinct = aggFunc.isDistinct();

            if (aggFunc instanceof Count && ((Count) aggFunc).isCountStar()) {
                sqlFunctionName = "COUNT";
                columnName = "*";
                distinct = false;  // count(*) has no distinct
            } else {
                Expression arg = aggFunc.child(0);
                SlotReference slot = (SlotReference) arg;
                String realColumnName = resolveColumnName(slot, project);
                if (realColumnName == null) {
                    LOG.info("JDBC_AGG_PUSHDOWN: cannot resolve column name for slot '{}' (complex expression)",
                            slot.getName());
                    return null;
                }
                columnName = realColumnName;
                switch (functionName) {
                    case "sum":
                        sqlFunctionName = "SUM";
                        break;
                    case "count":
                        sqlFunctionName = "COUNT";
                        break;
                    case "avg":
                        sqlFunctionName = "AVG";
                        break;
                    case "min":
                        sqlFunctionName = "MIN";
                        break;
                    case "max":
                        sqlFunctionName = "MAX";
                        break;
                    default:
                        LOG.info("JDBC_AGG_PUSHDOWN: function '{}' is not supported", functionName);
                        return null;
                }
            }

            String alias = aggOutputSlots.get(index).getName();
            aggregates.add(new ConnectorAggregate(sqlFunctionName, columnName, alias, distinct));
            LOG.info("JDBC_AGG_PUSHDOWN: built ConnectorAggregate: {}({}{}) AS {}",
                    sqlFunctionName, distinct ? "DISTINCT " : "", columnName, alias);
        }

        LOG.info("JDBC_AGG_PUSHDOWN: built {} aggregate(s), rewriting plan", aggregates.size());

        // Build new scan: output = aggregate output (reuse slots so upper operators
        // like having/order/limit keep their slot references valid), and attach the
        // pushdown aggregates so the ScanNode can apply them to the remote handle.
        return fileScan
                .withPushdownJdbcSimpleAggregates(aggregates)
                .withCachedOutput(aggregate.getOutput());
    }

    /**
     * Resolves the real column name for a slot reference, accounting for an optional
     * LogicalProject layer. Returns null if the slot maps to a complex expression
     * (which cannot be pushed down).
     */
    private String resolveColumnName(SlotReference slot, LogicalProject<?> project) {
        if (project == null) {
            return slot.getName();
        }
        for (NamedExpression expr : project.getProjects()) {
            if (expr.toSlot().getExprId().equals(slot.getExprId())) {
                // Unwrap Alias to get the underlying expression.
                Expression projected = expr instanceof Alias ? expr.child(0) : expr;
                if (projected instanceof SlotReference) {
                    return ((SlotReference) projected).getName();
                }
                return null;
            }
        }
        return slot.getName();
    }
}
