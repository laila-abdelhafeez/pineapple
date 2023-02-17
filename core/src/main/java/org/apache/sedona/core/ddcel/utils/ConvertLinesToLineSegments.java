package org.apache.sedona.core.ddcel.utils;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ConvertLinesToLineSegments implements FlatMapFunction<LineString, LineString> {
    @Override
    public Iterator<LineString> call(LineString lineString) throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        Set<LineString> lineSegments = new HashSet<LineString>();

        CoordinateSequence coordinates = lineString.getCoordinateSequence();

        for(int i = 0; i < coordinates.size()-1; ++i) {
            Coordinate[] lineSegmentCoordinates = new Coordinate[2];
            lineSegmentCoordinates[0] = coordinates.getCoordinate(i);
            lineSegmentCoordinates[1] = coordinates.getCoordinate(i+1);

            LineString lineSegment = geometryFactory.createLineString(lineSegmentCoordinates);
            lineSegments.add(lineSegment);
        }

        return lineSegments.iterator();
    }
}
