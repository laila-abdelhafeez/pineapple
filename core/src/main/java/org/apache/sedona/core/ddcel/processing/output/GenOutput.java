package org.apache.sedona.core.ddcel.processing.output;

import org.apache.sedona.core.ddcel.entries.*;

import java.util.Collection;

public class GenOutput extends RemOutput {

    private Collection<Vertex> vertices;
    private Collection<HalfEdge> halfEdges;
    public Collection<Vertex> getVertices() {
        return vertices;
    }

    public void setVertices(Collection<Vertex> vertices) {
        this.vertices = vertices;
    }

    public Collection<HalfEdge> getHalfEdges() {
        return halfEdges;
    }

    public void setHalfEdges(Collection<HalfEdge> halfEdges) {
        this.halfEdges = halfEdges;
    }

}

