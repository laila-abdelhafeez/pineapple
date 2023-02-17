package org.apache.sedona.core.ddcel.entries;


import org.locationtech.jts.geom.Coordinate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Cycle implements Serializable {

    private final List<DirectedEdge> edgesInCycle;

    public Cycle() {
        this.edgesInCycle = new ArrayList<>();
    }
    public void addToCycle(DirectedEdge edge) {
        this.edgesInCycle.add(edge);
    }
    public List<DirectedEdge> getEdgesInCycle() {
        return this.edgesInCycle;
    }
    public boolean contains(DirectedEdge edge) {
        return edgesInCycle.contains(edge);
    }
    public int indexOf(DirectedEdge edge) {
        return edgesInCycle.indexOf(edge);
    }
    public int size() {
        return this.edgesInCycle.size();
    }
    public double area() {
        int i = 0;
        int cycleLen = size();
        double area = 0;
        while (i+1 < cycleLen) {
            Coordinate p1 = edgesInCycle.get(i).source;
            Coordinate p2 = edgesInCycle.get(i+1).source;
            area += p1.x*p2.y - p2.x*p1.y;
            i++;
        }
        return area/2;
    }
    public DirectedEdge getLastEdge() {
        return edgesInCycle.get(edgesInCycle.size() - 1);
    }
    public DirectedEdge getFirstEdge() {
        return edgesInCycle.get(0);
    }
    public void addToRear(Cycle cycle) {
        edgesInCycle.addAll(cycle.edgesInCycle);
    }
    public void addToFront(Cycle cycle) {
        Cycle old = new Cycle();
        for(DirectedEdge oldEdge : edgesInCycle) old.addToCycle(oldEdge);
        updateCycle(cycle);
        updateCycle(old);
    }
    public boolean isSuperset(Cycle other) {
        return new HashSet<>(this.edgesInCycle).containsAll(other.edgesInCycle);
    }
    public boolean isSubset(Cycle other) {
        return new HashSet<>(other.edgesInCycle).containsAll(this.edgesInCycle);
    }
    public boolean hasInnerCycle() {
        for(int i = 0; i < edgesInCycle.size(); ++i) {
            for (int j = i+1; j < edgesInCycle.size(); ++j) {
                if(edgesInCycle.get(i).equals(edgesInCycle.get(j))) return true;
            }
        }
        return false;
    }
    public Cycle getInnerCycle() {
        for(int i = 0; i < edgesInCycle.size(); ++i) {
            for (int j = i+1; j < edgesInCycle.size(); ++j) {
                if(edgesInCycle.get(i).equals(edgesInCycle.get(j))) {
                    Cycle result = new Cycle();
                    for(int x = i; x <= j; ++x) {
                        result.addToCycle(edgesInCycle.get(x));
                    }
                    return result;

                }
            }
        }
        return null;
    }
    public void updateCycle(Cycle cycle) {
        edgesInCycle.clear();
        for(int i = 0; i < cycle.size(); ++i) {
            edgesInCycle.add(cycle.edgesInCycle.get(i));
        }
    }
    public boolean createsFace() {
        return edgesInCycle.get(0).equals(edgesInCycle.get(edgesInCycle.size()-1));
    }

    public Cycle getSubCycle(DirectedEdge edge) {
        int startingIndex = edgesInCycle.indexOf(edge);
        Cycle result = new Cycle();
        for(int i = startingIndex; i < edgesInCycle.size(); ++i) {
            result.addToCycle(edgesInCycle.get(i));
        }
        return result;
    }

    public Cycle removeFirst() {
        Cycle result = new Cycle();
        for(int i = 1; i < edgesInCycle.size(); ++i) {
            result.addToCycle(edgesInCycle.get(i));
        }
        return result;
    }
    public Cycle removeLast() {
        Cycle result = new Cycle();
        result.updateCycle(this);
        result.edgesInCycle.remove(edgesInCycle.size()-1);
        return result;
    }
    @Override
    public String toString() {
        StringBuilder cycle = new StringBuilder();
        for (DirectedEdge directedEdge: edgesInCycle) {
            cycle.append(directedEdge);
        }
        return cycle.toString();
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Cycle cycle = (Cycle) o;
        Set<DirectedEdge> edgesSet1 = new HashSet<>(edgesInCycle);
        Set<DirectedEdge> edgesSet2 = new HashSet<>(cycle.edgesInCycle);
        return edgesSet1.equals(edgesSet2);    }

    @Override
    public int hashCode() {
        return edgesInCycle.hashCode();
    }
}
