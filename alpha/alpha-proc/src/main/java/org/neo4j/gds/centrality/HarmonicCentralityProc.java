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
package org.neo4j.gds.centrality;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.NodePropertiesWriter;
import org.neo4j.gds.api.nodeproperties.DoubleNodeProperties;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.impl.closeness.HarmonicCentralityConfig;
import org.neo4j.gds.impl.harmonic.HarmonicCentrality;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class HarmonicCentralityProc extends NodePropertiesWriter<HarmonicCentrality, HarmonicCentrality, HarmonicCentralityConfig, HarmonicCentralityProc.StreamResult> {

    private static final String DESCRIPTION =
        "Harmonic centrality is a way of detecting nodes that are " +
        "able to spread information very efficiently through a graph.";

    @Procedure(name = "gds.alpha.closeness.harmonic.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);

        var algorithm = computationResult.algorithm();
        var graph = computationResult.graph();

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        return LongStream.range(0, graph.nodeCount())
            .boxed()
            .map(nodeId -> new StreamResult(graph.toOriginalNodeId(nodeId), algorithm.getCentralityScore(nodeId)));
    }

    @Procedure(value = "gds.alpha.closeness.harmonic.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<CentralityScore.Stats> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);

        var algorithm = computationResult.algorithm();
        var config = computationResult.config();
        var graph = computationResult.graph();

        AbstractCentralityResultBuilder<CentralityScore.Stats> builder = new CentralityScore.Stats.Builder(callContext, config.concurrency());

        builder
            .withNodeCount(graph.nodeCount())
            .withConfig(config)
            .withComputeMillis(computationResult.computeMillis())
            .withCreateMillis(computationResult.createMillis());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(builder.build());
        }

        builder.withCentralityFunction(computationResult.result()::getCentralityScore);

        try (ProgressTimer ignore = ProgressTimer.start(builder::withWriteMillis)) {
            var writeConcurrency = computationResult.config().writeConcurrency();
            var progressTracker = new TaskProgressTracker(
                NodePropertyExporter.baseTask("HarmonicCentrality", graph.nodeCount()),
                log,
                writeConcurrency,
                taskRegistryFactory
            );
            NodePropertyExporter exporter =  nodePropertyExporterBuilder
                .withIdMapping(graph)
                .withTerminationFlag(algorithm.getTerminationFlag())
                .withProgressTracker(progressTracker)
                .parallel(Pools.DEFAULT, writeConcurrency)
                .build();

            var properties = new DoubleNodeProperties() {
                @Override
                public long size() {
                    return computationResult.graph().nodeCount();
                }

                @Override
                public double doubleValue(long nodeId) {
                    return computationResult.result().getCentralityScore(nodeId);
                }
            };

            exporter.write(
                config.writeProperty(),
                properties
            );
        }

        return Stream.of(builder.build());
    }

    @Override
    protected HarmonicCentralityConfig newConfig(String username, CypherMapWrapper config) {
        return HarmonicCentralityConfig.of(config);
    }

    @Override
    protected GraphAlgorithmFactory<HarmonicCentrality, HarmonicCentralityConfig> algorithmFactory() {
        return new HarmonicCentralityAlgorithmFactory();
    }

    @SuppressWarnings("unused")
    public static final class StreamResult {
        public final long nodeId;
        public final double centrality;

        StreamResult(long nodeId, double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }
    }
}
