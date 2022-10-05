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
package org.neo4j.gds.paths.sourcetarget;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.paths.ShortestPathStreamProc;
import org.neo4j.gds.paths.StreamResult;
import org.neo4j.gds.paths.yens.config.ShortestPathYensStreamConfig;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.paths.yensastar.YensAStar;
import org.neo4j.gds.paths.yensastar.YensAStarFactory;
import org.neo4j.gds.paths.yensastar.config.ShortestPathYensAStarStreamConfig;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.paths.sourcetarget.ShortestPathYensAStarProc.YENS_ASTAR_DESCRIPTION;
import static org.neo4j.gds.paths.sourcetarget.ShortestPathYensProc.YENS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.shortestPath.yensastar.stream", description = YENS_ASTAR_DESCRIPTION, executionMode = STREAM)
public class ShortestPathYensAStarStreamProc extends ShortestPathStreamProc<YensAStar, ShortestPathYensAStarStreamConfig> {

    @Procedure(name = "gds.shortestPath.yensastar.stream", mode = READ)
    @Description(YENS_DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphName, configuration, false, true));
    }

    @Procedure(name = "gds.shortestPath.yensastar.stream.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected ShortestPathYensAStarStreamConfig newConfig(String username, CypherMapWrapper config) {
        return ShortestPathYensAStarStreamConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<YensAStar, ShortestPathYensAStarStreamConfig> algorithmFactory() {
        return new YensAStarFactory<>();
    }
}
