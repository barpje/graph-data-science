/*
 * Copyright (c) 2017-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo;


import org.assertj.core.api.Condition;
import org.assertj.core.api.HamcrestCondition;
import org.assertj.core.api.ObjectAssert;
import org.hamcrest.Matcher;
import org.intellij.lang.annotations.Language;
import org.neo4j.graphalgo.core.EnterpriseLicensingExtension;
import org.neo4j.graphalgo.core.GdsEdition;
import org.neo4j.graphalgo.core.utils.mem.AllocationTrackerExtensionFactory;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.Neo4jGraphExtension;
import org.neo4j.graphalgo.extension.NodeFunction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.runInTransaction;

@ImpermanentDbmsExtension(configurationCallback = "configuration")
@Neo4jGraphExtension
public abstract class BaseTest {

    @Inject
    public GraphDatabaseAPI db;

    @Inject
    public NodeFunction nodeFunction;

    @Inject
    public IdFunction idFunction;

    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        builder.impermanent();
        builder.noOpSystemGraphInitializer();
        builder.addExtension(new EnterpriseLicensingExtension());
        builder.addExtension(new AllocationTrackerExtensionFactory());
        // A change in 4.3.0-drop02.0 is enabling the feature to track cursor.close() events by default
        // for test databases. We would like to additionally enable the feature to trace cursors,
        // so that when we leak cursors, we can get a stacktrace of who was creating them.
        // This is a no-op in any version before 4.3.0-drop02.0, where this behavior is governed by feature toggles
        // but those are not enabled by default, test scope or otherwise.
        builder.setConfigRaw(Map.of("unsupported.dbms.debug.track_cursor_close", "true"));
        builder.setConfigRaw(Map.of("unsupported.dbms.debug.trace_cursors", "true"));
    }

    protected long clearDb() {
        var deletedNodes = new AtomicLong();
        runQueryWithRowConsumer("MATCH (n) DETACH DELETE n RETURN count(n)",
            row -> deletedNodes.set(row.getNumber("count(n)").longValue()));
        return deletedNodes.get();
    }

    protected void runQueryWithRowConsumer(
        @Language("Cypher") String query,
        Consumer<Result.ResultRow> check
    ) {
        QueryRunner.runQueryWithRowConsumer(db, query, check);
    }

    protected void runQueryWithRowConsumer(
        @Language("Cypher") String query,
        BiConsumer<Transaction, Result.ResultRow> check
    ) {
        QueryRunner.runQueryWithRowConsumer(db, query, emptyMap(), check);
    }

    protected void runQueryWithRowConsumer(
        @Language("Cypher") String query,
        Map<String, Object> params,
        Consumer<Result.ResultRow> check
    ) {
        QueryRunner.runQueryWithRowConsumer(db, query, params, discardTx(check));
    }

    protected void runQueryWithRowConsumer(
        GraphDatabaseService localDb,
        @Language("Cypher") String query,
        Map<String, Object> params,
        Consumer<Result.ResultRow> check
    ) {
        QueryRunner.runQueryWithRowConsumer(localDb, query, params, discardTx(check));
    }

    protected void runQueryWithRowConsumer(
        GraphDatabaseService localDb,
        @Language("Cypher") String query,
        Consumer<Result.ResultRow> check
    ) {
        QueryRunner.runQueryWithRowConsumer(localDb, query, emptyMap(), discardTx(check));
    }

    protected void runQueryWithRowConsumer(
        String username,
        @Language("Cypher") String query,
        Consumer<Result.ResultRow> check
    ) {
        QueryRunner.runQueryWithRowConsumer(db, username, query, emptyMap(), discardTx(check));
    }

    protected <T> T runQuery(
        String username,
        @Language("Cypher") String query,
        Function<Result, T> resultFunction
    ) {
        return QueryRunner.runQuery(db, username, query, emptyMap(), resultFunction);
    }

    protected void runQuery(
        String username,
        @Language("Cypher") String query,
        Map<String, Object> params
    ) {
        QueryRunner.runQuery(db, username, query, params);
    }

    protected void runQuery(
        String username,
        @Language("Cypher") String query
    ) {
        runQuery(username, query, Map.of());
    }

    protected void runQuery(
        GraphDatabaseService db,
        @Language("Cypher") String query,
        Map<String, Object> params
    ) {
        QueryRunner.runQuery(db, query, params);
    }

    protected void runQuery(@Language("Cypher") String query) {
        QueryRunner.runQuery(db, query);
    }

    protected void runQuery(
        @Language("Cypher") String query,
        Map<String, Object> params
    ) {
        QueryRunner.runQuery(db, query, params);
    }

    protected <T> T runQuery(
        @Language("Cypher") String query,
        Function<Result, T> resultFunction
    ) {
        return QueryRunner.runQuery(db, query, emptyMap(), resultFunction);
    }

    protected <T> T runQuery(
        @Language("Cypher") String query,
        Map<String, Object> params,
        Function<Result, T> resultFunction
    ) {
        return QueryRunner.runQuery(db, query, params, resultFunction);
    }

    protected <T> T runQuery(
        GraphDatabaseService db,
        @Language("Cypher") String query,
        Map<String, Object> params,
        Function<Result, T> resultFunction
    ) {
        return QueryRunner.runQuery(db, query, params, resultFunction);
    }

    protected void runQueryWithResultConsumer(
        @Language("Cypher") String query,
        Map<String, Object> params,
        Consumer<Result> check
    ) {
        QueryRunner.runQueryWithResultConsumer(
            db,
            query,
            params,
            check
        );
    }

    protected void runQueryWithResultConsumer(
        @Language("Cypher") String query,
        Consumer<Result> check
    ) {
        QueryRunner.runQueryWithResultConsumer(
            db,
            query,
            emptyMap(),
            check
        );
    }

    protected void runWithEnterpriseLicense(Runnable r) {
        var isOnEnterprise = GdsEdition.instance().isOnEnterpriseEdition();
        try {
            if (!isOnEnterprise) {
                GdsEdition.instance().setToEnterpriseEdition();
            }
            r.run();
        } finally {
            if (!isOnEnterprise) {
                GdsEdition.instance().setToCommunityEdition();
            }
        }
    }

    private static BiConsumer<Transaction, Result.ResultRow> discardTx(Consumer<Result.ResultRow> check) {
        return (tx, row) -> check.accept(row);
    }

    protected void assertCypherResult(@Language("Cypher") String query, List<Map<String, Object>> expected) {
        assertCypherResult(query, emptyMap(), expected);
    }

    @SuppressWarnings("unchecked")
    protected void assertCypherResult(
        @Language("Cypher") String query,
        Map<String, Object> queryParameters,
        List<Map<String, Object>> expected
    ) {
        runInTransaction(db, tx -> {
            List<Map<String, Object>> actual = new ArrayList<>();
            runQueryWithResultConsumer(query, queryParameters, result -> {
                result.accept(row -> {
                    Map<String, Object> _row = new HashMap<>();
                    for (String column : result.columns()) {
                        _row.put(column, row.get(column));
                    }
                    actual.add(_row);
                    return true;
                });
            });
            assertThat(actual)
                .withFailMessage("Different amount of rows returned for actual result (%d) than expected (%d)",
                    actual.size(),
                    expected.size()
                )
                .hasSize(expected.size());

            for (int i = 0; i < expected.size(); ++i) {
                Map<String, Object> expectedRow = expected.get(i);
                Map<String, Object> actualRow = actual.get(i);

                assertThat(actualRow.keySet()).containsExactlyInAnyOrderElementsOf(expectedRow.keySet());

                int rowNumber = i;
                expectedRow.forEach((key, expectedValue) -> {
                    Object actualValue = actualRow.get(key);
                    ObjectAssert<Object> assertion = assertThat(actualValue).withFailMessage(
                        "Different value for column '%s' of row %d (expected %s, but got %s)",
                        key,
                        rowNumber,
                        expectedValue,
                        actualValue
                    );

                    if (expectedValue instanceof Matcher) {
                        assertion.is(new HamcrestCondition<>((Matcher<Object>) expectedValue));
                    } else if (expectedValue instanceof Condition) {
                        assertion.is((Condition<Object>) expectedValue);
                    } else {
                        assertion.isEqualTo(expectedValue);
                    }
                });
            }
        });
    }
}
