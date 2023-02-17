package org.apache.sedona.core.ddcel.entries;

import org.locationtech.jts.geom.Coordinate;

import java.util.HashSet;
import java.util.Set;

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
    @Override
    public String toString() {
        return "Vertex{" + coordinates + '}';
    }

    @Override
    public boolean equals(Object object) {
        if(!(object instanceof Vertex)) return false;
        Vertex other = (Vertex) object;
        return this.coordinates == other.coordinates;
    }

}