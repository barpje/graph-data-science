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
package org.neo4j.gds.paths.yensastar.config;

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.paths.ShortestPathBaseConfig;

@ValueClass
@SuppressWarnings("immutables:subtype")
public interface ShortestPathYensAStarBaseConfig extends ShortestPathBaseConfig {

    String LONGITUDE_PROPERTY_KEY = "longitudeProperty";
    String LATITUDE_PROPERTY_KEY = "latitudeProperty";

    String ECONOMY_PRICE_PROPERTY_KEY = "economyPriceProperty";
    String BUSINESS_PRICE_KEY = "businessPriceProperty";

    String longitudeProperty();

    String latitudeProperty();

    String economyPriceProperty();
    String businessPriceProperty();

    // Number of shortest paths to compute
    @Configuration.IntegerRange(min = 1)
    int k();
}
