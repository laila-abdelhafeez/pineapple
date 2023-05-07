package org.apache.sedona.core.ddcel.entries;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HalfEdge implements DDCELEntry{

    private final DirectedEdge directedEdge;
    private DirectedEdge twin, next, prev;
    private Cycle incidentF;
    private boolean isDangle, isCutEdge;
    private double angle;
    private boolean spansMP;

    public HalfEdge(Coordinate source, Coordinate destination) {
        this.directedEdge = new DirectedEdge(source, destination);
        isDangle = false;
        isCutEdge = false;
        setAngle();
        setTwin();
    }
    public Coordinate getOrigin() {
        return directedEdge.source;
    }
    public Coordinate getDestination() {
        return directedEdge.destination;
    }
    public DirectedEdge getEdge() {
        return directedEdge;
    }
    public void setTwin() {
        this.twin = new DirectedEdge(getDestination(), getOrigin());
    }
    public DirectedEdge getTwin() {
        return twin;
    }
    public double getAngle() {
        return angle;
    }
    public void setAngle() {
        double dx = directedEdge.destination.x - directedEdge.source.x;
        double dy = directedEdge.destination.y - directedEdge.source.y;
        double len = Math.sqrt(dx*dx + dy*dy);

        if(dy > 0) this.angle = Math.acos(dx/len);
        else this.angle = 2*Math.PI - Math.acos(dx/len);
    }
    public boolean needsFace() {
        return !isDangle() && !isCutEdge() && incidentF == null;
    }
    public void setNext(DirectedEdge next) {
        this.next = next;
    }
    public DirectedEdge getNext() {
        return next;
    }
    public void setPrev(DirectedEdge prev) {
        this.prev = prev;
    }
    public DirectedEdge getPrev() {
        return prev;
    }
    public void setIncidentFace(Cycle face) {
        this.incidentF = face;
    }
    public Cycle getIncidentFace() {
        return incidentF;
    }
    public void setSpansMP() {
        this.spansMP = true;
    }
    public boolean isSpansMP() {
        return spansMP;
    }
    public void setDangle() {
        this.isDangle = true;
    }
    public boolean isDangle() {
        return isDangle;
    }
    public void setCutEdge() {
        this.isCutEdge = true;
    }
    public boolean isCutEdge() {
        return isCutEdge;
    }
    @Override
    public String toString() {
        return directedEdge + "\t" +
                twin + "\t" +
                next + "\t" +
                incidentF + "\t" +
                (isDangle ? "True" : "False") + "\t" +
                (isCutEdge ? "True" : "False") + "\t";
    }

    @Override
    public Geometry getGeometry() {
        return directedEdge.toLineString();
    }

    @Override
    public List<Object> getParams() {
        GeometryFactory factory = new GeometryFactory();
        return new ArrayList<>(Arrays.asList(twin.toLineString(), next == null? factory.createLineString() : next.toLineString(), incidentF == null? factory.createPolygon() : incidentF.toPolygon(), isDangle, isCutEdge));
    }
}