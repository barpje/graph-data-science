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
package org.neo4j.internal.recordstorage;

import org.neo4j.gds.compat.AbstractInMemoryRelationshipScanCursor;
import org.neo4j.storageengine.api.AllRelationshipsScan;

public class InMemoryRelationshipScan extends BaseRecordScan<AbstractInMemoryRelationshipScanCursor> implements AllRelationshipsScan {

    @Override
    boolean scanRange(AbstractInMemoryRelationshipScanCursor cursor, long start, long stopInclusive) {
        return cursor.scanRange(start, stopInclusive);
    }

    @Override
    public boolean scanBatch(int sizeHint, AbstractInMemoryRelationshipScanCursor cursor) {
        return super.scanBatch(sizeHint, cursor);
    }
}
