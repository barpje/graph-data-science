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
package org.neo4j.gds.betweenness;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;

interface ForwardTraversor {

    void traverse(long startNodeId);

    void clear();
}

class WeightedForwardTraversor implements ForwardTraversor {

    static WeightedForwardTraversor create(
        Graph graph,
        HugeObjectArray<LongArrayList> predecessors,
        HugeLongArrayStack backwardNodes,
        HugeLongArray sigma,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        var nodeCount = graph.nodeCount();
        var nodeQueue = HugeLongPriorityQueue.min(nodeCount);
        var visited = new BitSet(nodeCount);
        return new WeightedForwardTraversor(
            graph,
            predecessors,
            backwardNodes,
            sigma,
            nodeQueue,
            visited,
            terminationFlag,
            progressTracker
        );
    }

    private final Graph graph;
    private final TerminationFlag terminationFlag;
    private final ProgressTracker progressTracker;
    private final HugeLongArrayStack backwardNodes;
    private final HugeLongArray sigma;
    private final HugeLongPriorityQueue nodeQueue;
    private final HugeObjectArray<LongArrayList> predecessors;
    private final BitSet visited;

    private WeightedForwardTraversor(
        Graph graph,
        HugeObjectArray<LongArrayList> predecessors,
        HugeLongArrayStack backwardNodes,
        HugeLongArray sigma,
        HugeLongPriorityQueue nodeQueue,
        BitSet visited,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        this.predecessors = predecessors;
        this.backwardNodes = backwardNodes;
        this.sigma = sigma;
        this.nodeQueue = nodeQueue;
        this.visited = visited;
        this.graph = graph;
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
    }

    @Override
    public void traverse(long startNodeId) {
        nodeQueue.add(startNodeId, 0.0D);
        while (!nodeQueue.isEmpty() && terminationFlag.running()) {
            var node = nodeQueue.top();
            var thisNodesStoredCost = nodeQueue.cost(node);
            nodeQueue.pop();
            backwardNodes.push(node);
            visited.set(node);

            // For disconnected graphs, this will not reach 100%.
            progressTracker.logProgress(graph.degree(node));

            graph.forEachRelationship(
                node,
                1.0D,
                (source, target, weight) -> {
                    boolean visitedAlready = visited.get(target);
                    if (!visitedAlready) {
                        boolean insideQueue = nodeQueue.containsElement(target);
                        if (!insideQueue) {
                            nodeQueue.add(target, thisNodesStoredCost + weight);
                            var targetPredecessors = new LongArrayList();
                            predecessors.set(target, targetPredecessors);
                        }

                        if (nodeQueue.cost(target) == thisNodesStoredCost + weight) {
                            var pred = predecessors.get(target);
                            sigma.addTo(target, sigma.get(source));
                            pred.add(source);
                        } else if (weight + thisNodesStoredCost < nodeQueue.cost(target)) {
                            nodeQueue.set(target, weight + thisNodesStoredCost);
                            var pred = predecessors.get(target);
                            pred.clear();
                            sigma.set(target, sigma.get(source));
                            pred.add(source);
                        }
                    }
                    return true;
                }
            );
        }
    }

    @Override
    public void clear() {
        visited.clear();
    }
}

class UnweightedForwardTraversor implements ForwardTraversor {

    static UnweightedForwardTraversor create(
        Graph graph,
        HugeObjectArray<LongArrayList> predecessors,
        HugeLongArrayStack backwardNodes,
        HugeLongArray sigma,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        var nodeCount = graph.nodeCount();
        var distances = HugeIntArray.newArray(nodeCount);
        distances.fill(-1);
        var nodeQueue = HugeLongArrayQueue.newQueue(nodeCount);
        return new UnweightedForwardTraversor(
            graph,
            predecessors,
            backwardNodes,
            sigma,
            nodeQueue,
            distances,
            terminationFlag,
            progressTracker
        );
    }

    private final Graph graph;
    private final HugeObjectArray<LongArrayList> predecessors;
    private final HugeLongArrayStack backwardNodes;
    private final HugeLongArray sigma;
    private final HugeLongArrayQueue nodeQueue;
    private final HugeIntArray distances;
    private final TerminationFlag terminationFlag;
    private final ProgressTracker progressTracker;

    UnweightedForwardTraversor(
        Graph graph,
        HugeObjectArray<LongArrayList> predecessors,
        HugeLongArrayStack backwardNodes,
        HugeLongArray sigma,
        HugeLongArrayQueue nodeQueue,
        HugeIntArray distances,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        this.graph = graph;
        this.predecessors = predecessors;
        this.backwardNodes = backwardNodes;
        this.sigma = sigma;
        this.nodeQueue = nodeQueue;
        this.distances = distances;
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
    }

    @Override
    public void traverse(long startNodeId) {
        distances.set(startNodeId, 0);
        nodeQueue.add(startNodeId);

        while (!nodeQueue.isEmpty()) {
            long node = nodeQueue.remove();
            if (predecessors.get(node) != null)
                backwardNodes.push(node);
            int distanceNode = distances.get(node);

            graph.forEachRelationship(node, (source, target) -> {
                if (distances.get(target) < 0) {
                    nodeQueue.add(target);
                    distances.set(target, distanceNode + 1);
                }

                if (distances.get(target) == distanceNode + 1) {
                    sigma.addTo(target, sigma.get(source));
                    append(target, source);
                }
                return true;
            });
        }
    }

    @Override
    public void clear() {
        distances.fill(-1);
    }

    // append node to the path at target
    private void append(long target, long node) {
        LongArrayList targetPredecessors = predecessors.get(target);
        if (null == targetPredecessors) {
            targetPredecessors = new LongArrayList();
            predecessors.set(target, targetPredecessors);
        }
        targetPredecessors.add(node);
    }

}
