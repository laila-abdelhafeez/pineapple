package org.apache.sedona.core.ddcel.entries;

import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Face implements DDCELEntry {

    private final Cycle outerComponents;
    private boolean incompleteCycle;
    private double area = -1;

    public Face(Cycle cycle, boolean incompleteCycle) {
        this.outerComponents = cycle;
        this.incompleteCycle = incompleteCycle;
    }

    public Face(Cycle cycle) {
        this.outerComponents = cycle;
        this.incompleteCycle = false;
    }

    public Cycle getOuterComponents() {
        return outerComponents;
    }

    public void setCycleAsComplete() {
        this.incompleteCycle = false;
    }

    public boolean hasIncompleteCycle() {
        return incompleteCycle;
    }

    public DirectedEdge getLastEdge() {
        return outerComponents.getLastEdge();
    }
    public DirectedEdge getFirstEdge() {
        return outerComponents.getFirstEdge();
    }

    public void addToRear(Cycle cycle) {
        outerComponents.addToRear(cycle);
    }

    public void updateFace(Cycle cycle) {
        outerComponents.updateCycle(cycle);
    }
    public double getArea() {
        if(area != -1) return area;
        area = outerComponents.area();
        return area;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Face face = (Face) o;
        return outerComponents.equals(face.outerComponents);
    }
    @Override
    public int hashCode() {
        return outerComponents.hashCode();
    }
    @Override
    public String toString() {
        return outerComponents.toString();
    }

    @Override
    public Geometry getGeometry() {
        return outerComponents.toPolygon();
    }

    @Override
    public List<Object> getParams() {
        return new ArrayList<>(Collections.singletonList(area));
    }
}