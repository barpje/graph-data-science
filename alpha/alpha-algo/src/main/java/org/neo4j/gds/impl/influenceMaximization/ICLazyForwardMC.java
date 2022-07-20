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
package org.neo4j.gds.impl.influenceMaximization;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

final class ICLazyForwardMC {


    private final int monteCarloSimulations;

    private final int concurrency;

    private final List<ICLazyForwardTask> tasks;

    private final ExecutorService executorService;

    static ICLazyForwardMC create(
        Graph graph,
        double propagationProbability,
        int monteCarloSimulations,
        long[] seedSetNodes,
        int concurrency,
        ExecutorService executorService,
        long initialRandomSeed
    ) {

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            monteCarloSimulations,
            // should we copy the array when we initialise the threads?
            partition -> new ICLazyForwardTask(
                partition,
                graph.concurrentCopy(),
                seedSetNodes.clone(),
                propagationProbability,
                initialRandomSeed
            ),
            Optional.of(monteCarloSimulations / concurrency)
        );
        return new ICLazyForwardMC(monteCarloSimulations, tasks, concurrency, executorService);
    }

    ICLazyForwardMC(
        int monteCarloSimulations,
        List<ICLazyForwardTask> tasks,
        int concurrency,
        ExecutorService executorService
    ) {
//
        this.monteCarloSimulations = monteCarloSimulations;

        this.tasks = tasks;

        this.concurrency = concurrency;
        this.executorService = executorService;
    }

    double runForCandidate(long candidateId) {
        for (var task : tasks) {
            task.setCandidateNodeId(candidateId);
        }
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .executor(executorService)
            .run();
        double spread = 0d;
        for (var task : tasks) {
            spread += task.getSpread();
        }
        return spread / monteCarloSimulations;
    }

    void incrementSeedNode(long newSetNode) {
        for (var task : tasks) {
            task.incrementSeedNode(newSetNode);
        }
    }

}
