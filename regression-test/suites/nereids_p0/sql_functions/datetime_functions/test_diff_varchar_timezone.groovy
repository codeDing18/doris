// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor agreements.  See the NOTICE file
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

// Regression test for issue #66120:
// DATEDIFF and all *_diff functions used to bind varchar slots (real varchar column,
// subquery projection slot, UNION-ALL output slot) to the TIMESTAMPTZ overload.
// Under a non-UTC session time_zone the varchar -> timestamptz cast shifts the value
// to UTC, so calendar diff functions silently return off-by-one results.
// After the fix, non-literal string slots must bind to DateTimeV2 (wall-clock semantics)
// and the result must be independent of the session time_zone.
suite("test_diff_varchar_timezone") {
    sql "SET enable_nereids_planner = true;"
    sql "SET enable_fallback_to_original_planner = false;"

    def tableName = "diff_varchar_tz"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            a VARCHAR(30),
            b VARCHAR(30),
            dt_a DATETIME(6),
            dt_b DATETIME(6),
            d_a DATE,
            d_b DATE,
            tz_a TIMESTAMPTZ(6),
            tz_b TIMESTAMPTZ(6)
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1');
    """
    sql """
        INSERT INTO ${tableName} VALUES
        (1, '2025-08-01 00:00:00', '2025-05-30 11:40:09',
            '2025-08-01 00:00:00', '2025-05-30 11:40:09',
            '2025-08-01', '2025-05-30',
            CAST('2025-08-01 00:00:00 +08:00' AS TIMESTAMPTZ(6)),
            CAST('2025-05-30 11:40:09 +08:00' AS TIMESTAMPTZ(6))),
        (2, '2025-07-31 10:00:00', '2025-07-30 08:30:00',
            '2025-07-31 10:00:00', '2025-07-30 08:30:00',
            '2025-07-31', '2025-07-30',
            CAST('2025-07-31 10:00:00 +08:00' AS TIMESTAMPTZ(6)),
            CAST('2025-07-30 08:30:00 +08:00' AS TIMESTAMPTZ(6)));
    """

    // --- 1. numeric assertions: results must be independent of session time_zone ---

    def diffQueries = [
        "SELECT DATEDIFF(a, b) FROM ${tableName} WHERE id = 1",
        "SELECT DATEDIFF(a, b) FROM (SELECT '2025-08-01 00:00:00' AS a, '2025-05-30 11:40:09' AS b) t",
        """SELECT DATEDIFF(a, b) FROM (
             SELECT '2025-08-01 00:00:00' AS a, '2025-05-30 11:40:09' AS b
             UNION ALL SELECT '2025-08-02 00:00:00', '2024-09-30 19:40:02'
           ) t ORDER BY 1""",
        "SELECT DATEDIFF(dt_a, dt_b) FROM ${tableName} WHERE id = 1",
        "SELECT DATEDIFF(d_a, d_b) FROM ${tableName} WHERE id = 1"
    ]

    def resultsByTz = [:]
    for (tz in ["+00:00", "+08:00", "America/Los_Angeles", "Asia/Shanghai"]) {
        sql "SET time_zone = '${tz}'"
        def results = diffQueries.collect { q -> sql(q).collect { row -> row[0].toString() } }
        resultsByTz[tz] = results
    }

    // anchor: expected wall-clock values under every time_zone
    for (tz in ["+00:00", "+08:00", "America/Los_Angeles", "Asia/Shanghai"]) {
        def r = resultsByTz[tz]
        assertEquals("63", r[0][0], "varchar column under tz ${tz}")
        assertEquals("63", r[1][0], "subquery slot under tz ${tz}")
        assertEquals(["63", "306"], r[2], "union-all slot under tz ${tz}")
        assertEquals("63", r[3][0], "datetime column under tz ${tz}")
        assertEquals("63", r[4][0], "date column under tz ${tz}")
    }

    // all 12 *_diff functions on varchar columns: wall-clock expected values,
    // a='2025-08-01 00:00:00' b='2025-05-30 11:40:09' -> 62 days 12:19:51
    def diffFns = ["datediff", "days_diff", "weeks_diff", "months_diff", "quarters_diff", "years_diff",
                   "timediff", "hours_diff", "minutes_diff", "seconds_diff",
                   "milliseconds_diff", "microseconds_diff"]
    // timediff is clamped by BE to the TIME upper bound 838:59:59
    // (TimeDiffImpl -> TimeValue::limit_with_bound, be/src/core/value/time_value.h),
    // the real diff 62 days 12:19:51 = 1500:19:51 exceeds it
    def expected = ["datediff": "63",
                    "days_diff": "62",
                    "weeks_diff": "8",
                    "months_diff": "2",
                    "quarters_diff": "0",
                    "years_diff": "0",
                    "timediff": "838:59:59",
                    "hours_diff": "1500",
                    "minutes_diff": "90019",
                    "seconds_diff": "5401191",
                    "milliseconds_diff": "5401191000",
                    "microseconds_diff": "5401191000000"]
    def allFnsSelect = diffFns.collect { fn -> "${fn}(a, b)" }.join(", ")
    for (tz in ["+00:00", "+08:00", "America/Los_Angeles", "Asia/Shanghai"]) {
        sql "SET time_zone = '${tz}'"
        def row = sql("SELECT ${allFnsSelect} FROM ${tableName} WHERE id = 1")[0]
        diffFns.eachWithIndex { fn, i ->
            assertEquals(expected[fn], row[i].toString(), "${fn} on varchar column under tz ${tz}")
        }
    }

    // row 2 gives a timediff below the TIME bound: 2025-07-31 10:00:00 - 2025-07-30 08:30:00 = 25:30:00
    for (tz in ["+00:00", "+08:00", "America/Los_Angeles", "Asia/Shanghai"]) {
        sql "SET time_zone = '${tz}'"
        def timeDiffRow = sql("SELECT timediff(a, b) FROM ${tableName} WHERE id = 2")[0]
        assertEquals("25:30:00", timeDiffRow[0].toString(), "timediff (below TIME bound) under tz ${tz}")
    }

    // --- 2. EXPLAIN assertions: which overload gets bound ---

    // a pure-varchar table so that no timestamptz column type can leak into explain output
    def varcharTable = "diff_varchar_only"
    sql "DROP TABLE IF EXISTS ${varcharTable}"
    sql """
        CREATE TABLE ${varcharTable} (
            id INT,
            a VARCHAR(30),
            b VARCHAR(30)
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1');
    """
    sql """
        INSERT INTO ${varcharTable} VALUES (1, '2025-08-01 00:00:00', '2025-05-30 11:40:09');
    """

    // varchar column / subquery slot / union slot / bare literals must NOT be cast to TIMESTAMPTZ
    for (q in ["SELECT DATEDIFF(a, b) FROM ${varcharTable}",
               "SELECT timediff(a, b), hours_diff(a, b) FROM ${varcharTable}",
               "SELECT DATEDIFF(a, b) FROM (SELECT '2025-08-01 00:00:00' AS a, '2025-05-30 11:40:09' AS b) t",
               "SELECT DATEDIFF(a, b) FROM (SELECT a FROM ${varcharTable} UNION ALL SELECT a FROM ${varcharTable}) t",
               "SELECT DATEDIFF('2025-08-01 00:00:00', '2025-05-30 11:40:09')"]) {
        def explain = sql "EXPLAIN ${q}"
        assertTrue(!explain.toString().toUpperCase().contains("TIMESTAMPTZ"),
                "non-literal string input must not bind the timestamptz overload: ${q}")
    }

    // an explicit timestamptz cast must still bind the TIMESTAMPTZ overload
    def explainExplicitCast = sql """
        EXPLAIN SELECT DATEDIFF(CAST('2025-08-01 00:00:00 +08:00' AS TIMESTAMPTZ(6)),
                                CAST('2025-05-30 11:40:09 +08:00' AS TIMESTAMPTZ(6)))
    """
    assertTrue(explainExplicitCast.toString().toUpperCase().contains("TIMESTAMPTZ"),
            "explicit timestamptz cast must keep the timestamptz overload")

    // tz-aware literals and timestamptz columns keep instant semantics:
    // 2025-08-01 00:00 +08:00 = UTC 2025-07-31 16:00, 2025-05-30 11:40:09 +08:00 = UTC 2025-05-30 03:40:09,
    // so DATEDIFF is 62 in every session time_zone
    for (tz in ["+00:00", "+08:00", "America/Los_Angeles"]) {
        sql "SET time_zone = '${tz}'"
        def litDiff = sql "SELECT DATEDIFF('2025-08-01 00:00:00 +08:00', '2025-05-30 11:40:09 +08:00')"
        assertEquals("62", litDiff[0][0].toString(), "tz-aware literal diff under tz ${tz}")
        def tzColDiff = sql "SELECT DATEDIFF(tz_a, tz_b) FROM ${tableName} WHERE id = 1"
        assertEquals("62", tzColDiff[0][0].toString(), "timestamptz column diff under tz ${tz}")
    }

    sql "DROP TABLE IF EXISTS ${varcharTable}"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "SET time_zone = default"
}
