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

import org.apache.doris.catalog.Column;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.AggregateApplicationResult;
import org.apache.doris.connector.api.pushdown.ConnectorAggregate;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Pushes simple aggregate functions (SUM/COUNT/AVG/MIN/MAX) down to JDBC catalogs
 * (currently MySQL only) for global aggregates (no GROUP BY).
 *
 * <p>Supported shapes (with optional LogicalFilter and LogicalProject layers):
 * <ul>
 *   <li>{@code aggregate(scan)}
 *   <li>{@code aggregate(project(scan))}
 *   <li>{@code aggregate(filter(scan))}
 *   <li>{@code aggregate(project(filter(scan)))}
 * </ul>
 *
 * <p>The rule collects supported aggregate functions into {@link ConnectorAggregate}
 * objects, attaches them to the scan via {@code pushdownJdbcSimpleAggregates}, and
 * replaces the aggregate (and optional project) with the scan alone. If a filter is
 * present, it is kept above the scan so its conjuncts are applied as the JDBC WHERE
 * clause during scan-node finalization. The scan's output is the aggregate's output
 * so that upper operators (having/order/limit) keep valid.
 *
 * <p>Conservatively bails out if any function or argument is unsupported.
 */
public class PushDownAggregateToJdbcScan implements RewriteRuleFactory {

    private static final Logger LOG = LogManager.getLogger(PushDownAggregateToJdbcScan.class);

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // Shape 1: aggregate(scan)
                logicalAggregate(logicalFileScan().when(this::isJdbcCatalog))
                        .then(agg -> tryPushDown((LogicalAggregate<? extends Plan>) agg))
                        .toRule(RuleType.JDBC_AGGREGATE_PUSHDOWN),
                // Shape 2: aggregate(project(scan))
                logicalAggregate(logicalProject(logicalFileScan().when(this::isJdbcCatalog)))
                        .then(agg -> tryPushDown((LogicalAggregate<? extends Plan>) agg))
                        .toRule(RuleType.JDBC_AGGREGATE_PUSHDOWN));
    }

    /**
     * Tries to push down the aggregate to a JDBC file scan. Returns null (no rewrite)
     * when the session variable is disabled, any aggregate is unsupported, or the
     * scan is not on a JDBC catalog.
     *
     * <p>Supports global aggregates (no GROUP BY) and single-column GROUP BY.
     */
    private Plan tryPushDown(LogicalAggregate<? extends Plan> aggregate) {
        LOG.info("JDBC_AGG_PUSHDOWN: tryPushDown called, output={}, childClass={}",
                aggregate.getOutput(), aggregate.child(0).getClass().getSimpleName());
        if (!enableJdbcPushDownAggregate()) {
            LOG.info("JDBC_AGG_PUSHDOWN: skip, enable_jdbc_pushdown_aggregate=false");
            return null;
        }
        // Supports global aggregates (no GROUP BY) and multi-column GROUP BY.
        // ROLLUP/CUBE/GROUPING SETS produce a LogicalRepeat node (not LogicalAggregate),
        // so this rule never matches them — no extra check needed.
        List<Expression> groupByExprs = aggregate.getGroupByExpressions();
        if (!aggregate.getDistinctArguments().isEmpty()) {
            LOG.info("JDBC_AGG_PUSHDOWN: skip, aggregate has DISTINCT arguments");
            return null;
        }

        // Unwrap optional LogicalProject layer to reach the file scan.
        Plan child = aggregate.child(0);
        LogicalProject<? extends Plan> project = null;

        if (child instanceof LogicalProject) {
            project = (LogicalProject<? extends Plan>) child;
            child = project.child(0);
        }
        if (!(child instanceof LogicalFileScan)) {
            LOG.info("JDBC_AGG_PUSHDOWN: skip, leaf child is not LogicalFileScan but {}",
                    child.getClass().getSimpleName());
            return null;
        }
        LogicalFileScan fileScan = (LogicalFileScan) child;

        // Resolve group-by column names (every group-by expression must be a plain column).
        List<String> groupByColumns = new ArrayList<>();
        for (Expression gb : groupByExprs) {
            if (!(gb instanceof SlotReference)) {
                LOG.info("JDBC_AGG_PUSHDOWN: skip, group-by expression is not a plain column: {}", gb);
                return null;
            }
            String gbColName = resolveColumnName((SlotReference) gb, project);
            if (gbColName == null) {
                LOG.info("JDBC_AGG_PUSHDOWN: skip, cannot resolve group-by column name");
                return null;
            }
            groupByColumns.add(gbColName);
        }

        LOG.info("JDBC_AGG_PUSHDOWN: matching aggregate with output={}, hasProject={}, groupBy={}",
                aggregate.getOutput(), project != null, groupByColumns);

        // Build the ConnectorAggregate list (returns null if unsupported).
        // groupBySlots collects the group-by output slots (they go into newOutput but
        // are not wrapped in virtual columns, unlike aggregate function outputs).
        List<Slot> outputSlots = new ArrayList<>();
        List<Slot> groupBySlots = new ArrayList<>();
        List<ConnectorAggregate> aggregates =
                buildConnectorAggregates(aggregate, project, outputSlots, groupBySlots);
        if (aggregates == null) {
            LOG.info("JDBC_AGG_PUSHDOWN: skip, some aggregate function or argument is unsupported");
            return null;
        }

        // Get a fresh connector handle and apply aggregate via SPI.
        ConnectorTableHandle handle = resolveAndApplyPushdown(fileScan, aggregates, groupByColumns);
        if (handle == null) {
            LOG.info("JDBC_AGG_PUSHDOWN: skip, failed to apply aggregate to connector handle");
            return null;
        }

        // Build new scan output. Group-by columns reuse the original Column (they are
        // base-table columns); aggregate function outputs get virtual columns.
        List<Slot> newOutput = new ArrayList<>();
        for (Slot slot : outputSlots) {
            SlotReference slotRef = (SlotReference) slot;
            if (groupBySlots.contains(slot)) {
                // group-by column: keep the original Column backing the slot.
                newOutput.add(slotRef);
            } else {
                Column virtualColumn = new Column(slot.getName(),
                        slotRef.getDataType().toCatalogDataType(), true);
                newOutput.add(slotRef.withColumn(virtualColumn));
            }
        }

        LOG.info("JDBC_AGG_PUSHDOWN: pushed down to JDBC scan for table {}, handle={}",
                fileScan.getTable() == null ? "null" : fileScan.getTable().getName(), handle);
        return fileScan
                .withPushdownJdbcHandle(handle)
                .withCachedOutput(newOutput);
    }

    /**
     * Resolves a fresh ConnectorTableHandle from the connector and applies aggregates via SPI.
     * Returns the updated handle, or null on failure.
     */
    private ConnectorTableHandle resolveAndApplyPushdown(LogicalFileScan fileScan,
            List<ConnectorAggregate> aggregates, List<String> groupByColumns) {
        PluginDrivenExternalTable table = (PluginDrivenExternalTable) fileScan.getTable();
        PluginDrivenExternalCatalog catalog = (PluginDrivenExternalCatalog) table.getCatalog();
        Connector connector = catalog.getConnector();
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        String dbName = table.getDb() != null ? table.getDb().getRemoteName() : "";
        String tableName = table.getRemoteName();
        ConnectorTableHandle handle = metadata.getTableHandle(session, dbName, tableName)
                .orElse(null);
        if (handle == null) {
            LOG.info("JDBC_AGG_PUSHDOWN: resolveAndApplyPushdown - table handle not found for {}.{}", dbName, tableName);
            return null;
        }
        LOG.info("JDBC_AGG_PUSHDOWN: resolveAndApplyPushdown - fresh handle={}", handle);

        Optional<AggregateApplicationResult<ConnectorTableHandle>> result =
                metadata.applyAggregate(session, handle, aggregates, groupByColumns);
        if (!result.isPresent()) {
            LOG.info("JDBC_AGG_PUSHDOWN: resolveAndApplyPushdown - applyAggregate rejected by connector");
            return null;
        }
        handle = result.get().getHandle();
        LOG.info("JDBC_AGG_PUSHDOWN: resolveAndApplyPushdown - applyAggregate succeeded, final handle={}", handle);
        return handle;
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
     * Returns true if the aggregate function is structurally pushable:
     * a single SlotReference argument, or COUNT(*).
     * <p>The actual function-level support (e.g. whether MySQL accepts this
     * function) is decided later by the connector via
     * {@code JdbcConnectorClient.supportsAggregatePushdown}.
     */
    private boolean isSupported(AggregateFunction func) {
        if (func instanceof Count && ((Count) func).isCountStar()) {
            return true;
        }
        return func.arity() == 1 && func.child(0) instanceof SlotReference;
    }

    /**
     * Parses aggregate output expressions into ConnectorAggregate objects.
     * Returns null if any aggregate function or argument is unsupported.
     *
     * <p>Populates {@code outputSlots} with all output slots in order (group-by columns
     * first, then aggregate functions — matching LogicalAggregate's output layout), and
     * {@code groupBySlots} with only the group-by column slots (so the caller knows which
     * output slots are base-table columns vs. virtual aggregate columns).
     */
    private List<ConnectorAggregate> buildConnectorAggregates(LogicalAggregate<? extends Plan> aggregate,
            LogicalProject<? extends Plan> project, List<Slot> outputSlots, List<Slot> groupBySlots) {
        List<ConnectorAggregate> aggregates = new ArrayList<>();
        for (NamedExpression outputExpr : aggregate.getOutputExpressions()) {
            Set<AggregateFunction> funcs = outputExpr.collect(AggregateFunction.class::isInstance);
            Slot slot = outputExpr.toSlot();
            if (funcs.isEmpty()) {
                // Group-by column: a bare SlotReference in the output (no aggregate function).
                // It is pushed down as a plain SELECT/GROUP BY column, not a ConnectorAggregate.
                groupBySlots.add(slot);
                outputSlots.add(slot);
                continue;
            }
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

            // Pass the nereids function name (lowercase, e.g. "sum"); the connector decides
            // whether it is supported via JdbcConnectorClient.supportsAggregatePushdown.
            String functionName = aggFunc.getName().toLowerCase();
            String columnName;
            ConnectorType columnType = null;
            boolean distinct = aggFunc.isDistinct();

            if (aggFunc instanceof Count && ((Count) aggFunc).isCountStar()) {
                columnName = "*";
                distinct = false;  // count(*) has no distinct
            } else {
                Expression arg = aggFunc.child(0);
                SlotReference argSlot = (SlotReference) arg;
                String realColumnName = resolveColumnName(argSlot, project);
                if (realColumnName == null) {
                    LOG.info("JDBC_AGG_PUSHDOWN: cannot resolve column name for slot '{}' (complex expression)",
                            argSlot.getName());
                    return null;
                }
                columnName = realColumnName;
                columnType = toConnectorType(argSlot.getDataType());
            }

            aggregates.add(new ConnectorAggregate(functionName, columnName, slot.getName(), distinct, columnType));
            outputSlots.add(slot);
            LOG.info("JDBC_AGG_PUSHDOWN: built ConnectorAggregate: {}({}{}) AS {} type={}",
                    functionName, distinct ? "DISTINCT " : "", columnName, slot.getName(), columnType);
        }

        LOG.info("JDBC_AGG_PUSHDOWN: built {} aggregate(s), {} group-by col(s), rewriting plan",
                aggregates.size(), groupBySlots.size());
        return aggregates;
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

    /**
     * Converts a nereids {@link DataType} to a connector-side {@link ConnectorType},
     * so the connector can make type-aware pushdown decisions (e.g. {@code AVG} on
     * integer vs decimal, {@code MIN}/{@code MAX} on textual types).
     * Returns null for types not relevant to aggregate pushdown.
     */
    private static ConnectorType toConnectorType(DataType dt) {
        if (dt instanceof DecimalV3Type) {
            DecimalV3Type decimal = (DecimalV3Type) dt;
            return ConnectorType.of("DECIMALV3", decimal.getPrecision(), decimal.getScale());
        }
        if (dt instanceof BigIntType) {
            return ConnectorType.of("BIGINT");
        }
        if (dt instanceof LargeIntType) {
            return ConnectorType.of("LARGEINT");
        }
        if (dt instanceof IntegerType) {
            return ConnectorType.of("INT");
        }
        if (dt instanceof SmallIntType) {
            return ConnectorType.of("SMALLINT");
        }
        if (dt instanceof TinyIntType) {
            return ConnectorType.of("TINYINT");
        }
        if (dt instanceof DoubleType) {
            return ConnectorType.of("DOUBLE");
        }
        if (dt instanceof FloatType) {
            return ConnectorType.of("FLOAT");
        }
        if (dt instanceof VarcharType) {
            return ConnectorType.of("VARCHAR", ((CharacterType) dt).getLen(), -1);
        }
        if (dt instanceof StringType) {
            // MySQL TEXT/TINYTEXT/MEDIUMTEXT/LONGTEXT map to Doris StringType.
            return ConnectorType.of("STRING");
        }
        if (dt instanceof CharacterType) {
            return ConnectorType.of("CHAR", ((CharacterType) dt).getLen(), -1);
        }
        return ConnectorType.of(dt.toString().toUpperCase());
    }
}
