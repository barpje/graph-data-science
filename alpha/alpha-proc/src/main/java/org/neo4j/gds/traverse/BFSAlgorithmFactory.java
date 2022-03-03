/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.traverse;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.impl.traverse.Aggregator;
import org.neo4j.gds.impl.traverse.BFS;
import org.neo4j.gds.impl.traverse.BfsConfig;
import org.neo4j.gds.impl.traverse.ExitPredicate;

import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.InputNodeValidator.validateEndNode;
import static org.neo4j.gds.utils.InputNodeValidator.validateStartNode;

class BFSAlgorithmFactory extends GraphAlgorithmFactory<BFS, BfsConfig> {

    @Override
    public BFS build(
        Graph graphOrGraphStore, BfsConfig configuration, ProgressTracker progressTracker
    ) {
        validateStartNode(configuration.sourceNode(), graphOrGraphStore);
        configuration.targetNodes().forEach(neoId -> validateEndNode(neoId, graphOrGraphStore));

        ExitPredicate exitFunction;
        Aggregator aggregatorFunction;
        // target node given; terminate if target is reached
        if (!configuration.targetNodes().isEmpty()) {
            List<Long> mappedTargets = configuration.targetNodes().stream()
                .map(graphOrGraphStore::safeToMappedNodeId)
                .collect(Collectors.toList());
            exitFunction = (s, t, w) -> mappedTargets.contains(t) ? ExitPredicate.Result.BREAK : ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;
            // maxDepth given; continue to aggregate nodes with lower depth until no more nodes left
        } else if (configuration.maxDepth() != -1) {
            exitFunction = (s, t, w) -> w > configuration.maxDepth() ? ExitPredicate.Result.CONTINUE : ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> w + 1.;
            // do complete BFS until all nodes have been visited
        } else {
            exitFunction = (s, t, w) -> ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;
        }
        var mappedStartNodeId = graphOrGraphStore.toMappedNodeId(configuration.sourceNode());

        return new BFS(
            graphOrGraphStore,
            mappedStartNodeId,
            exitFunction,
            aggregatorFunction,
            configuration.concurrency(),
            progressTracker
        );
    }

    @Override
    public String taskName() {
        return "BFS";
    }
}
