package org.apache.sedona.core.sgpac;

import org.apache.sedona.core.sgpac.enums.PolygonState;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

public class PolygonClip {

    public static Geometry createEnvelopeGeometry(Envelope envelope) {
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(envelope.getMinX(), envelope.getMinY());
        coordinates[1] = new Coordinate(envelope.getMinX(), envelope.getMaxY());
        coordinates[2] = new Coordinate(envelope.getMaxX(), envelope.getMaxY());
        coordinates[3] = new Coordinate(envelope.getMaxX(), envelope.getMinY());
        coordinates[4] = coordinates[0];

        GeometryFactory geometryFactory = new GeometryFactory();
        return geometryFactory.createPolygon(coordinates);
    }


    public static Tuple2<PolygonState, Geometry> clip(Envelope node, Geometry polygon) {

        PolygonState state;
        Geometry clippedPolygon = null;

        try {

            Geometry nodeGeometry = createEnvelopeGeometry(node);

            if(!node.intersects(polygon.getEnvelopeInternal()) && !polygon.intersects(nodeGeometry.getCentroid())) {
                state = PolygonState.OUTSIDE;
            } else {
                clippedPolygon = polygon.intersection(nodeGeometry);

                if (!clippedPolygon.isEmpty()) {
                    if(clippedPolygon.equalsExact(nodeGeometry)) {
                        state = PolygonState.WITHIN;
                    }
                    else {
                        state = PolygonState.INTERSECT;
                    }
                } else {
                    // node is a line or point
                    if (polygon.intersects(nodeGeometry.getCentroid()) ) {
                        state = PolygonState.WITHIN;
                        clippedPolygon = nodeGeometry;
                    } else {
                        state = PolygonState.OUTSIDE;
                    }
                }
            }

        } catch (Exception ignore) {
            state = PolygonState.INVALID;
        }

        return new Tuple2<>(state, clippedPolygon);
    }
}
