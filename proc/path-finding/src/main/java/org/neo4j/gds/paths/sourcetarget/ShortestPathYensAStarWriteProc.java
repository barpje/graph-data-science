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
import org.neo4j.gds.paths.ShortestPathWriteProc;
import org.neo4j.gds.paths.yens.Yens;
import org.neo4j.gds.paths.yens.YensFactory;
import org.neo4j.gds.paths.yens.config.ShortestPathYensWriteConfig;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.paths.yensastar.YensAStar;
import org.neo4j.gds.paths.yensastar.YensAStarFactory;
import org.neo4j.gds.paths.yensastar.config.ShortestPathYensAStarWriteConfig;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardWriteRelationshipsResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.paths.sourcetarget.ShortestPathYensProc.YENS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.WRITE_RELATIONSHIP;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

@GdsCallable(name = "gds.shortestPath.yensastar.write", description = YENS_DESCRIPTION, executionMode = WRITE_RELATIONSHIP)
public class ShortestPathYensAStarWriteProc extends ShortestPathWriteProc<YensAStar, ShortestPathYensAStarWriteConfig> {

    @Procedure(name = "gds.shortestPath.yensastar.write", mode = WRITE)
    @Description(YENS_DESCRIPTION)
    public Stream<StandardWriteRelationshipsResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphName, configuration, false, true));
    }

    @Procedure(name = "gds.shortestPath.yensastar.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected ShortestPathYensAStarWriteConfig newConfig(String username, CypherMapWrapper config) {
        return ShortestPathYensAStarWriteConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<YensAStar, ShortestPathYensAStarWriteConfig> algorithmFactory() {
        return new YensAStarFactory<>();
    }
}
