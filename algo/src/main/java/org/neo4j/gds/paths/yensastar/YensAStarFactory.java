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
package org.neo4j.gds.paths.yensastar;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.paths.yensastar.YensAStar;
import org.neo4j.gds.paths.yensastar.config.ShortestPathYensAStarBaseConfig;
import org.neo4j.gds.paths.dijkstra.DijkstraFactory;

import java.util.List;

public class YensAStarFactory<CONFIG extends ShortestPathYensAStarBaseConfig> extends GraphAlgorithmFactory<YensAStar, CONFIG> {

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return YensAStar.memoryEstimation();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.task(
                taskName(),
                Tasks.iterativeDynamic(
                        "Searching path",
                        () -> List.of(Tasks.iterativeOpen(
                                "k",
                                () -> List.of(DijkstraFactory.dijkstraProgressTask(graph))
                        )),
                        config.k()
                )
        );
    }

    @Override
    public String taskName() {
        return "YensAStar";
    }

    @Override
    public YensAStar build(
        Graph graph,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        return YensAStar.sourceTarget(graph, configuration, progressTracker);
    }
}
