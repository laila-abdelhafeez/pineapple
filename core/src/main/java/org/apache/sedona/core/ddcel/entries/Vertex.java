package org.apache.sedona.core.ddcel.entries;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.util.*;
import java.util.stream.Collectors;

public class Vertex implements DDCELEntry {

    private final Coordinate coordinates;
    private final Set<DirectedEdge> incidentH;
    private boolean isDangle;

    public Vertex(Coordinate coordinates) {
        this.coordinates = coordinates;
        this.incidentH = new HashSet<>();
        this.isDangle = false;
    }

    public Coordinate getCoordinate() {
        return coordinates;
    }
    public void addIncidentEdge(DirectedEdge incidentEdge) {
        this.incidentH.add(incidentEdge);
    }
    public void addIncidentEdges(Iterable<DirectedEdge> incidentEdgesIds) {
        for (DirectedEdge incidentEdgeId : incidentEdgesIds) {
            addIncidentEdge(incidentEdgeId);
        }
    }
    public int getDegree() {
        return incidentH.size();
    }
    public Set<DirectedEdge> getIncidentEdges() {
        return incidentH;
    }
    public boolean isDangle() {
        return isDangle;
    }
    public void setAsDangle() {
        isDangle = true;
    }

    public Point toPoint() {
        GeometryFactory factory = new GeometryFactory();
        return factory.createPoint(coordinates);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(toPoint().toString()).append("\t");
        for(DirectedEdge incident : incidentH) {
            builder.append(incident).append("\t");
        }
        return builder.toString();
    }

    @Override
    public boolean equals(Object object) {
        if(!(object instanceof Vertex)) return false;
        Vertex other = (Vertex) object;
        return this.coordinates == other.coordinates;
    }

    @Override
    public Geometry getGeometry() {
        return toPoint();
    }

    @Override
    public List<Object> getParams() {
        return incidentH.stream().map(DirectedEdge::toLineString).collect(Collectors.toList());
    }
}