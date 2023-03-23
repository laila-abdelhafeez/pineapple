package org.apache.sedona.core.sgpac;

import org.apache.sedona.core.sgpac.enums.PolygonState;
import org.locationtech.jts.geom.*;
import scala.Serializable;
import scala.Tuple2;

public class PolygonClip implements Serializable {

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

    public static Point createPointGeometry(double x, double y) {
        GeometryFactory geometryFactory = new GeometryFactory();
        return geometryFactory.createPoint(new Coordinate(x, y));
    }

    public static LineString createLineGeometry(Envelope envelope) {
        Coordinate[] coordinates = new Coordinate[2];
        coordinates[0] = new Coordinate(envelope.getMinX(), envelope.getMinY());
        coordinates[1] = new Coordinate(envelope.getMaxX(), envelope.getMaxY());
        GeometryFactory geometryFactory = new GeometryFactory();
        return geometryFactory.createLineString(coordinates);
    }

    public static boolean isPoint(Envelope envelope) {
        return envelope.getMinX() == envelope.getMaxX() && envelope.getMinY() == envelope.getMaxY();
    }

    public static boolean isLine(Envelope envelope) {
        return !isPoint(envelope) && (envelope.getMinX() == envelope.getMaxX() || envelope.getMinY() == envelope.getMaxY());
    }


    public static Tuple2<PolygonState, Geometry> clip(Envelope node, Geometry polygon) {

        PolygonState state;
        Geometry clippedPolygon = null;

        if(isPoint(node)) {
            Point point = createPointGeometry(node.getMinX(), node.getMinY());
            if(polygon.contains(point) || polygon.intersects(point)){
                clippedPolygon = point;
                state = PolygonState.WITHIN;
            } else {
                state = PolygonState.OUTSIDE;
            }
        } else if(isLine(node)) {
            LineString line = createLineGeometry(node);
            if(polygon.contains(line)){
                clippedPolygon = line;
                state = PolygonState.WITHIN;
            } else if(polygon.intersects(line)) {
                clippedPolygon = polygon.intersection(line);
                state = PolygonState.INTERSECT;
            } else {
                state = PolygonState.OUTSIDE;
            }
        } else {
            Geometry nodeGeometry = createEnvelopeGeometry(node);

            if(polygon.getEnvelopeInternal().disjoint(node) || polygon.disjoint(nodeGeometry)) {
                state = PolygonState.OUTSIDE;
            } else if(polygon.contains(nodeGeometry)) {
                clippedPolygon = nodeGeometry;
                state = PolygonState.WITHIN;
            } else {
                clippedPolygon = polygon.intersection(nodeGeometry);
                if(clippedPolygon.equalsExact(nodeGeometry)) {
                    state = PolygonState.WITHIN;
                } else {
                    state = PolygonState.INTERSECT;
                }
            }
        }

        return new Tuple2<>(state, clippedPolygon);
    }
}
