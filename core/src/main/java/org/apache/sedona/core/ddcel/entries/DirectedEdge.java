package org.apache.sedona.core.ddcel.entries;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

import java.io.Serializable;

public class DirectedEdge implements Serializable {

    public Coordinate source, destination;

    public DirectedEdge(Coordinate source, Coordinate destination) {
        this.source = source;
        this.destination = destination;
    }

    @Override
    public boolean equals(Object object) {
        if(!(object instanceof DirectedEdge)) return false;
        DirectedEdge other = (DirectedEdge) object;
        return source.equals2D(other.source) && destination.equals2D(other.destination);
    }

    @Override
    public int hashCode() {
        return source.hashCode() + destination.hashCode();
    }
    public LineString toLineString() {
        GeometryFactory factory = new GeometryFactory();
        Coordinate[] coordinates = new Coordinate[2];
        coordinates[0] = source;
        coordinates[1] = destination;
        return factory.createLineString(coordinates);
    }

    @Override
    public String toString() {
        return "{(" + source.x + ", " + source.y + ") -> (" + destination.x + ", " + destination.y + ")}";
    }

}
