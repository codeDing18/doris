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

import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorType;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NotEqualTo;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts Nereids filter conjuncts to ConnectorExpression for JDBC pushdown.
 * Minimal converter supporting comparison predicates, AND, and literals.
 */
final class NereidsToConnectorExpression {

    private NereidsToConnectorExpression() {
    }

    /**
     * Converts a list of conjuncts to a single AND-ed ConnectorExpression.
     * Returns null if any conjunct is unsupported.
     */
    static ConnectorExpression convertConjuncts(List<Expression> conjuncts) {
        if (conjuncts == null || conjuncts.isEmpty()) {
            return null;
        }
        List<ConnectorExpression> converted = new ArrayList<>();
        for (Expression expr : conjuncts) {
            ConnectorExpression ce = convert(expr);
            if (ce == null) {
                return null;
            }
            converted.add(ce);
        }
        return converted.size() == 1 ? converted.get(0) : new ConnectorAnd(converted);
    }

    static ConnectorExpression convert(Expression expr) {
        if (expr instanceof SlotReference) {
            return new ConnectorColumnRef(((SlotReference) expr).getName(), ConnectorType.UNKNOWN);
        }
        if (expr instanceof Literal) {
            return convertLiteral((Literal) expr);
        }
        if (expr instanceof ComparisonPredicate) {
            return convertComparison((ComparisonPredicate) expr);
        }
        if (expr instanceof org.apache.doris.nereids.trees.expressions.And) {
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
        return null;
    }

    private static ConnectorExpression convertLiteral(Literal lit) {
        if (lit instanceof org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral) {
            return new org.apache.doris.connector.api.pushdown.ConnectorLiteral(
                    ConnectorType.BIGINT,
                    ((org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral) lit).getLong());
        }
        if (lit instanceof org.apache.doris.nereids.trees.expressions.literal.StringLiteral) {
            return new org.apache.doris.connector.api.pushdown.ConnectorLiteral(
                    ConnectorType.VARCHAR,
                    ((org.apache.doris.nereids.trees.expressions.literal.StringLiteral) lit).getValue().toString());
        }
        if (lit instanceof org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral) {
            return new org.apache.doris.connector.api.pushdown.ConnectorLiteral(
                    ConnectorType.VARCHAR,
                    ((org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral) lit).getValue());
        }
        if (lit instanceof org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral) {
            return new org.apache.doris.connector.api.pushdown.ConnectorLiteral(
                    ConnectorType.BOOLEAN,
                    ((org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral) lit).getValue());
        }
        if (lit instanceof org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral) {
            return new org.apache.doris.connector.api.pushdown.ConnectorLiteral(
                    ConnectorType.DECIMAL,
                    new java.math.BigDecimal(
                            ((org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral) lit).getValue()));
        }
        return new org.apache.doris.connector.api.pushdown.ConnectorLiteral(
                ConnectorType.VARCHAR, lit.getStringValue());
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
        } else {
            return null;
        }
        return new ConnectorComparison(op, left, right);
    }
}
