package org.neo4j.gds.paths.yensastar.heuristic;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeLongDoubleMap;
import org.neo4j.gds.paths.dijkstra.Dijkstra;

public class PriceHeuristic implements Dijkstra.HeuristicFunction {

    private final double targetBusinessPrice;
    private final double targetEconomyPrice;

    private final NodePropertyValues economyPrice;
    private final NodePropertyValues businessPrice;

    public PriceHeuristic(
            NodePropertyValues economyPrice,
            NodePropertyValues businessPrice,
            long targetNode
    ) {
        this.economyPrice = economyPrice;
        this.businessPrice = businessPrice;
        this.targetEconomyPrice = economyPrice.doubleValue(targetNode);
        this.targetBusinessPrice = businessPrice.doubleValue(targetNode);
    }

    @Override
    public double applyAsDouble(long source) {
        return targetEconomyPrice + targetBusinessPrice;
    }
}