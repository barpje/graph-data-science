package org.neo4j.gds.paths.yensastar.heuristic;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeLongDoubleMap;
import org.neo4j.gds.paths.dijkstra.Dijkstra;

public class HaversineHeuristic implements Dijkstra.HeuristicFunction {

    static final double DEFAULT_DISTANCE = Double.NaN;
    // kilometer to nautical mile
    static final double KM_TO_NM = 0.539957;
    static final double EARTH_RADIUS_IN_NM = 6371 * KM_TO_NM;

    private final double targetLatitude;
    private final double targetLongitude;

    private final NodePropertyValues latitudeProperties;
    private final NodePropertyValues longitudeProperties;

    private final HugeLongDoubleMap distanceCache;

    public HaversineHeuristic(
            NodePropertyValues latitudeProperties,
            NodePropertyValues longitudeProperties,
            long targetNode
    ) {
        this.latitudeProperties = latitudeProperties;
        this.longitudeProperties = longitudeProperties;
        this.targetLatitude = latitudeProperties.doubleValue(targetNode);
        this.targetLongitude = longitudeProperties.doubleValue(targetNode);
        this.distanceCache = new HugeLongDoubleMap();
    }

    @Override
    public double applyAsDouble(long source) {
        var distance = distanceCache.getOrDefault(source, DEFAULT_DISTANCE);

        if (Double.isNaN(distance)) {
            var sourceLatitude = latitudeProperties.doubleValue(source);
            var sourceLongitude = longitudeProperties.doubleValue(source);
            distance = distance(sourceLatitude, sourceLongitude, targetLatitude, targetLongitude);
            distanceCache.addTo(source, distance);
        }

        return distance;
    }

    // https://rosettacode.org/wiki/Haversine_formula#Java
    public static double distance(
            double sourceLatitude,
            double sourceLongitude,
            double targetLatitude,
            double targetLongitude
    ) {
        var latitudeDistance = Math.toRadians(targetLatitude - sourceLatitude);
        var longitudeDistance = Math.toRadians(targetLongitude - sourceLongitude);
        var lat1 = Math.toRadians(sourceLatitude);
        var lat2 = Math.toRadians(targetLatitude);

        var a = Math.pow(Math.sin(latitudeDistance / 2), 2)
                + Math.pow(Math.sin(longitudeDistance / 2), 2)
                * Math.cos(lat1) * Math.cos(lat2);

        var c = 2 * Math.asin(Math.sqrt(a));

        return EARTH_RADIUS_IN_NM * c;
    }
}