package org.neo4j.gds.paths.yensastar.heuristic;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeLongDoubleMap;
import org.neo4j.gds.paths.dijkstra.Dijkstra;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;

public class FlightsHeuristic implements Dijkstra.HeuristicFunction {

    static final double DEFAULT_DISTANCE = Double.NaN;
    static final double HEURISTIC_INF = 0.04;

    // kilometer to nautical mile
    static final double KM_TO_NM = 0.539957;
    static final double EARTH_RADIUS_IN_NM = 6371 * KM_TO_NM;

    // flight estimation
    static final double NM_TO_MILES = 1.1508;
    static final double AVERAGE_CRUISE_SPEED_0_400 = 166; // in nautical miles per hour
    static final double AVERAGE_CRUISE_SPEED_400_1000 = 287; // in nautical miles per hour
    static final double AVERAGE_CRUISE_SPEED_1000_2500 = 398; // in nautical miles per hour
    static final double AVERAGE_CRUISE_SPEED_2500_4000 = 432.5; // in nautical miles per hour
    static final double AVERAGE_CRUISE_SPEED_4000_UP = 444.6; // in nautical miles per hour

    static final double MAX_FLIGHT_TIME = 18.50; // hours
    static final double NOTHING_CHANGE = 0; // wrong node type or matched requirements indicator

    private final double targetLatitude;
    private final double targetLongitude;

    private final NodePropertyValues latitudeProperties;
    private final NodePropertyValues longitudeProperties;
    // private final NodePropertyValues arrivalProperties;
    private final NodePropertyValues departureProperties;
    private final NodePropertyValues seatsProperties;
    //private final NodePropertyValues businessSeatsProperties;
    private final HugeLongDoubleMap distanceCache;

    //private final String cabinType;
    private final LocalDate departureDate;
    //private final double arrivalDate;
    private final int requestedSeats;

    public FlightsHeuristic(
            NodePropertyValues latitudeProperties,
            NodePropertyValues longitudeProperties,
            //NodePropertyValues arrivalProperties,
            NodePropertyValues departureProperties,
            NodePropertyValues seatsProperties,
            //NodePropertyValues businessSeatsProperties,
            //String cabinType,
            LocalDate departureDate,
            //double arrivalDate,
            int requestedSeats,
            long targetNode
    ) {
        this.latitudeProperties = latitudeProperties;
        this.longitudeProperties = longitudeProperties;
        // this.arrivalProperties = arrivalProperties;
        this.departureProperties = departureProperties;
        this.seatsProperties = seatsProperties;
        // this.businessSeatsProperties = businessSeatsProperties;
        this.targetLatitude = latitudeProperties.doubleValue(targetNode);
        this.targetLongitude = longitudeProperties.doubleValue(targetNode);
        // this.cabinType = cabinType;
        this.departureDate = departureDate;
        // this.arrivalDate = arrivalDate;
        this.requestedSeats = requestedSeats;
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
        final double heuristicCut = checkDate(source) + checkAvl(source);
        return getHeuristicDuration(distance) + heuristicCut;
    }

    public double getHeuristicDuration(double distance){
        if (distance <= 400) {
            return distance / AVERAGE_CRUISE_SPEED_0_400 / MAX_FLIGHT_TIME;
        } else if (distance > 400 && distance <= 1000) {
            return distance / AVERAGE_CRUISE_SPEED_400_1000 / MAX_FLIGHT_TIME;
        } else if (distance > 1000 && distance <= 2500) {
            return distance / AVERAGE_CRUISE_SPEED_1000_2500 / MAX_FLIGHT_TIME;
        } else if (distance > 2500 && distance <= 4000) {
            return distance / AVERAGE_CRUISE_SPEED_2500_4000 / MAX_FLIGHT_TIME;
        }else return distance / AVERAGE_CRUISE_SPEED_4000_UP / MAX_FLIGHT_TIME;
    }

    public double checkAvl(long source) {
        if (seatsProperties.value(source) == null) {
            return NOTHING_CHANGE;  // nothing change, different node type
        }
        var seats = seatsProperties.longValue(source);
        if (seats < requestedSeats) {
            return HEURISTIC_INF;
        }
        return NOTHING_CHANGE;
    }

    public double checkDate(long source) {
        if (departureProperties.value(source) == null) {
            return NOTHING_CHANGE;  // nothing change, different node type
        }
        // var arrival = arrivalProperties.doubleValue(source);
        var departure = departureProperties.doubleValue(source);
        LocalDate departureLd = LocalDate.ofEpochDay(Duration.ofMillis(Double.valueOf(departure).longValue()).toDays());
        if (departureLd.isBefore(departureDate)) {
            return HEURISTIC_INF;
        }
        return NOTHING_CHANGE;
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