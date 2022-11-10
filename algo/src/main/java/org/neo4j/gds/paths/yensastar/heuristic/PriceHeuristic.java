package org.neo4j.gds.paths.yensastar.heuristic;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeLongDoubleMap;
import org.neo4j.gds.paths.dijkstra.Dijkstra;

public class PriceHeuristic implements Dijkstra.HeuristicFunction {

    static final double DEFAULT_PRICE = Double.NaN;

    private final double targetBusinessPrice;
    private final double targetEconomyPrice;
    private final long targetEconomySeats;


    private final NodePropertyValues economyPrice;
    private final NodePropertyValues businessPrice;
    private final NodePropertyValues economySeats;


    private final HugeLongDoubleMap priceCache;

    public PriceHeuristic(
            NodePropertyValues economyPrice,
            NodePropertyValues businessPrice,
            NodePropertyValues economySeats,
            long targetNode
    ) {
        this.economyPrice = economyPrice;
        this.economySeats = economySeats;
        this.businessPrice = businessPrice;
        this.targetEconomySeats = economySeats.longValue(targetNode);
        this.targetEconomyPrice = economyPrice.doubleValue(targetNode);
        this.targetBusinessPrice = businessPrice.doubleValue(targetNode);
        this.priceCache = new HugeLongDoubleMap();
    }

    @Override
    public double applyAsDouble(long source) {
        var price = priceCache.getOrDefault(source, DEFAULT_PRICE);

        if (Double.isNaN(price)) {
            price = targetEconomyPrice;
            priceCache.addTo(source, price);
        }
        if (economySeats.longValue(source) == 0){
            return 100000;
        }
        return price > 0 ? price : 1000000;
    }
}