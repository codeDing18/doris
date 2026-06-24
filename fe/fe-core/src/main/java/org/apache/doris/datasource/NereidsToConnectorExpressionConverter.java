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

package org.apache.doris.datasource;

import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorBetween;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorNot;
import org.apache.doris.connector.api.pushdown.ConnectorOr;
import org.apache.doris.connector.api.pushdown.ConnectorType;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NotEqualTo;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts Nereids {@link Expression} trees to {@link ConnectorExpression} trees,
 * bypassing the legacy {@code Expr} hierarchy. Used for JDBC aggregate pushdown
 * where the filter conjuncts reference slots that are not in the scan's tuple descriptor.
 */
public final class NereidsToConnectorExpressionConverter {

    private NereidsToConnectorExpressionConverter() {
    }

    /**
     * Converts a single Nereids Expression to a ConnectorExpression.
     * Returns null for unsupported expression types.
     */
    public static ConnectorExpression convert(Expression expr) {
        if (expr instanceof SlotReference) {
            return convertSlotReference((SlotReference) expr);
        }
        if (expr instanceof Literal) {
            return convertLiteral((Literal) expr);
        }
        if (expr instanceof ComparisonPredicate) {
            return convertComparison((ComparisonPredicate) expr);
        }
        if (expr instanceof And) {
            List<ConnectorExpression> children = new ArrayList<>();
            for (Expression child : expr.children()) {
                ConnectorExpression ce = convert(child);
                if (ce == null) {
                    return null;
                }
                children.add(ce);
            }
            return new ConnectorAnd(children);
        }
        if (expr instanceof Or) {
            List<ConnectorExpression> children = new ArrayList<>();
            for (Expression child : expr.children()) {
                ConnectorExpression ce = convert(child);
                if (ce == null) {
                    return null;
                }
                children.add(ce);
            }
            return new ConnectorOr(children);
        }
        if (expr instanceof Not) {
            ConnectorExpression child = convert(expr.child(0));
            return child == null ? null : new ConnectorNot(child);
        }
        if (expr instanceof IsNull) {
            ConnectorExpression child = convert(expr.child(0));
            return child == null ? null : new ConnectorIsNull(child, false);
        }
        if (expr instanceof InPredicate) {
            return convertInPredicate((InPredicate) expr);
        }
        if (expr instanceof Between) {
            return convertBetween((Between) expr);
        }
        return null;
    }

    private static ConnectorExpression convertSlotReference(SlotReference slot) {
        return new ConnectorColumnRef(slot.getName(), ConnectorType.UNKNOWN);
    }

    private static ConnectorExpression convertLiteral(Literal lit) {
        if (lit instanceof NullLiteral) {
            return ConnectorLiteral.ofNull(ConnectorType.UNKNOWN);
        }
        if (lit instanceof BooleanLiteral) {
            return new ConnectorLiteral(ConnectorType.BOOLEAN, ((BooleanLiteral) lit).getValue());
        }
        if (lit instanceof IntegerLiteral) {
            return new ConnectorLiteral(ConnectorType.BIGINT,
                    ((IntegerLiteral) lit).getLong());
        }
        if (lit instanceof DecimalLiteral) {
            return new ConnectorLiteral(ConnectorType.DECIMAL,
                    new BigDecimal(((DecimalLiteral) lit).getValue()));
        }
        if (lit instanceof StringLiteral) {
            return new ConnectorLiteral(ConnectorType.VARCHAR,
                    ((StringLiteral) lit).getValue().toString());
        }
        if (lit instanceof VarcharLiteral) {
            return new ConnectorLiteral(ConnectorType.VARCHAR,
                    ((VarcharLiteral) lit).getValue());
        }
        if (lit instanceof StringLikeLiteral) {
            return new ConnectorLiteral(ConnectorType.VARCHAR,
                    ((StringLikeLiteral) lit).getValue());
        }
        if (lit instanceof DateLiteral) {
            DateLiteral dl = (DateLiteral) lit;
            LocalDate ld = LocalDate.of(
                    (int) dl.getYear(), (int) dl.getMonth(), (int) dl.getDay());
            return new ConnectorLiteral(ConnectorType.DATE, ld);
        }
        return new ConnectorLiteral(ConnectorType.VARCHAR, lit.getStringValue());
    }

    private static ConnectorExpression convertComparison(ComparisonPredicate pred) {
        ConnectorExpression left = convert(pred.child(0));
        ConnectorExpression right = convert(pred.child(1));
        if (left == null || right == null) {
            return null;
        }
        ConnectorComparison.Operator op;
        if (pred instanceof EqualTo) {
            op = ConnectorComparison.Operator.EQ;
        } else if (pred instanceof NotEqualTo) {
            op = ConnectorComparison.Operator.NE;
        } else if (pred instanceof GreaterThan) {
            op = ConnectorComparison.Operator.GT;
        } else if (pred instanceof GreaterThanEqual) {
            op = ConnectorComparison.Operator.GE;
        } else if (pred instanceof LessThan) {
            op = ConnectorComparison.Operator.LT;
        } else if (pred instanceof LessThanEqual) {
            op = ConnectorComparison.Operator.LE;
        } else if (pred instanceof NullSafeEqual) {
            op = ConnectorComparison.Operator.IS_NOT_DISTINCT_FROM;
        } else {
            return null;
        }
        return new ConnectorComparison(op, left, right);
    }

    private static ConnectorExpression convertInPredicate(InPredicate in) {
        ConnectorExpression value = convert(in.child(0));
        if (value == null) {
            return null;
        }
        List<ConnectorExpression> inList = new ArrayList<>();
        for (Expression option : in.getOptions()) {
            ConnectorExpression ce = convert(option);
            if (ce == null) {
                return null;
            }
            inList.add(ce);
        }
        // Nereids InPredicate does not carry NOT IN flag; NOT IN is represented as Not(InPredicate)
        return new ConnectorIn(value, inList, false);
    }

    private static ConnectorExpression convertBetween(Between between) {
        ConnectorExpression value = convert(between.child(0));
        ConnectorExpression lower = convert(between.child(1));
        ConnectorExpression upper = convert(between.child(2));
        if (value == null || lower == null || upper == null) {
            return null;
        }
        // Nereids Between does not carry NOT BETWEEN flag; NOT BETWEEN is represented as Not(Between)
        return new ConnectorBetween(value, lower, upper);
    }
}
