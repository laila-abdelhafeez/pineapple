package org.apache.sedona.core.ddcel.processing;

import org.apache.sedona.core.ddcel.entries.*;
import org.apache.sedona.core.ddcel.enums.RemMethod;
import org.apache.sedona.core.ddcel.processing.output.GenOutput;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.LineString;

import java.util.*;

public class GenPhase implements FlatMapFunction<Iterator<LineString>, GenOutput> {

    private final RemMethod remMethod;
    private final List<Envelope> partitionGrids;
    private Envelope boundary;
    Map<Coordinate, Vertex> partitionVertices = new HashMap<>();
    Map<DirectedEdge, HalfEdge> partitionHalfEdges = new HashMap<>();
    Set<Face> partitionFaces = new HashSet<>();

    public GenPhase(List<Envelope> partitionGrids, RemMethod remMethod) {
        this.partitionGrids = partitionGrids;
        this.remMethod = remMethod;
    }

    private void addVertex(LineString lineString) {
        Coordinate[] coordinates = lineString.getCoordinates();
        assert coordinates.length == 2;

        if(boundary.contains(coordinates[0])) {
            partitionVertices.putIfAbsent(coordinates[0], new Vertex(coordinates[0]));
            partitionVertices.get(coordinates[0]).addIncidentEdge(new DirectedEdge(coordinates[1], coordinates[0]));
        }

        if(boundary.contains(coordinates[1])) {
            partitionVertices.putIfAbsent(coordinates[1], new Vertex(coordinates[1]));
            partitionVertices.get(coordinates[1]).addIncidentEdge(new DirectedEdge(coordinates[0], coordinates[1]));
        }
    }

    private void addHalfEdge(LineString lineString) {
        Coordinate[] coordinates = lineString.getCoordinates();
        assert coordinates.length == 2;

        HalfEdge halfEdge = new HalfEdge(coordinates[0], coordinates[1]);
        HalfEdge twinEdge = new HalfEdge(coordinates[1], coordinates[0]);

        if(!boundary.contains(coordinates[0]) || !boundary.contains(coordinates[1])) {
            halfEdge.setSpansMP();
            twinEdge.setSpansMP();
        }
        partitionHalfEdges.putIfAbsent(halfEdge.getEdge(), halfEdge);
        partitionHalfEdges.putIfAbsent(twinEdge.getEdge(), twinEdge);
    }

    private boolean checkVertexIsNewDangle(Vertex vertex) {
        if(vertex.isDangle()) return false; // already a dangle

        int degreeIn = 0;
        for(DirectedEdge edge: vertex.getIncidentEdges()) {
            if(partitionHalfEdges.get(edge).isDangle()) continue;
            ++degreeIn;
        }
        return degreeIn <= 1 ;
    }
    private long markCurrentDangles() {
        long numberOfDangles = 0;
        for(Vertex vertex: partitionVertices.values()) {
            if(checkVertexIsNewDangle(vertex)) {
                partitionVertices.get(vertex.getCoordinate()).setAsDangle();
                for(DirectedEdge edge: vertex.getIncidentEdges()) {
                    partitionHalfEdges.get(edge).setDangle();
                    partitionHalfEdges.get(partitionHalfEdges.get(edge).getTwin()).setDangle();
                }
                ++numberOfDangles;
            }
        }
        return numberOfDangles;
    }
    private List<HalfEdge> sortIncidentEdges(Iterable<DirectedEdge> incidentEdges) {
        List<HalfEdge> halfEdges = new ArrayList<>();
        for (DirectedEdge incidentEdge: incidentEdges) {
            if(!partitionHalfEdges.get(incidentEdge).isCutEdge() && !partitionHalfEdges.get(incidentEdge).isDangle()) {
                halfEdges.add(partitionHalfEdges.get(incidentEdge));
            }
        }
        halfEdges.sort(Comparator.comparingDouble(HalfEdge::getAngle).reversed());
        return halfEdges;
    }
    private void setNextAndPreviousForIncidentEdges(Vertex vertex) {
        if(vertex.isDangle()) return;

        List<HalfEdge> sortedIncidentEdges = sortIncidentEdges(vertex.getIncidentEdges());
        for(int i = 0; i+1 < sortedIncidentEdges.size(); ++i) {
            partitionHalfEdges.get(sortedIncidentEdges.get(i).getEdge()).setNext(sortedIncidentEdges.get(i+1).getTwin());
            partitionHalfEdges.get(sortedIncidentEdges.get(i+1).getEdge()).setPrev(sortedIncidentEdges.get(i).getEdge());
        }
        partitionHalfEdges.get(sortedIncidentEdges.get(sortedIncidentEdges.size()-1).getEdge()).setNext(sortedIncidentEdges.get(0).getTwin());
        partitionHalfEdges.get(sortedIncidentEdges.get(0).getEdge()).setPrev(sortedIncidentEdges.get(sortedIncidentEdges.size()-1).getEdge());
    }
    private boolean updateVertex(Vertex vertex) {
        if(vertex.isDangle()) return false;

        int numCutEdges = 0;
        int numDangles = 0;
        for(DirectedEdge edge: vertex.getIncidentEdges()) {
            if(partitionHalfEdges.get(edge).isCutEdge()) ++numCutEdges;
            if(partitionHalfEdges.get(edge).isDangle()) ++numDangles;
        }
        return (vertex.getDegree()-numDangles-numCutEdges > 1);
    }
    private void markCutEdges() {

        Set<DirectedEdge> visited = new HashSet<>();
        for(HalfEdge startingHalfEdge: partitionHalfEdges.values()) {

            if(startingHalfEdge.isDangle() || startingHalfEdge.isCutEdge()) continue;
            if(visited.contains(startingHalfEdge.getEdge())) continue;

            Cycle cycle = new Cycle();
            cycle.addToCycle(startingHalfEdge.getEdge());
            visited.add(startingHalfEdge.getEdge());

            DirectedEdge current = startingHalfEdge.getNext();
            while (current != startingHalfEdge.getEdge() && !cycle.contains(current) && partitionHalfEdges.containsKey(current)) {
                HalfEdge currentHalfEdge = partitionHalfEdges.get(current);
                visited.add(current);
                if(cycle.contains(currentHalfEdge.getTwin())) {
                    partitionHalfEdges.get(current).setCutEdge();
                    partitionHalfEdges.get(currentHalfEdge.getTwin()).setCutEdge();
                    break;
                }
                cycle.addToCycle(current);
                current = partitionHalfEdges.get(current).getNext();
            }
        }
    }

    private void GenRH() {

        Set<DirectedEdge> visited = new HashSet<>();

        for(HalfEdge startingHalfEdge: partitionHalfEdges.values()) {

            if(!startingHalfEdge.needsFace()) continue;

            Cycle cycle = new Cycle();
            cycle.addToCycle(startingHalfEdge.getEdge());

            DirectedEdge next = startingHalfEdge.getNext();

            while (next != null && !next.equals(startingHalfEdge.getEdge()) && !cycle.contains(next) && partitionHalfEdges.get(next).needsFace() && !visited.contains(next)) {
                cycle.addToCycle(next);
                next = partitionHalfEdges.get(next).getNext();
            }

            if (next == null) {

                if(!partitionHalfEdges.get(cycle.getLastEdge()).isSpansMP()) {
                    for(DirectedEdge edge: cycle.getEdgesInCycle()) {
                        partitionHalfEdges.get(edge).setDangle();
                    }
                }
                else {
                    visited.addAll(cycle.getEdgesInCycle());
                }
            } else if(visited.contains(next)) {
                visited.addAll(cycle.getEdgesInCycle());
            }
            else if(!partitionHalfEdges.get(next).needsFace()) {
                for(DirectedEdge edge: cycle.getEdgesInCycle()) {
                    partitionHalfEdges.get(edge).setDangle();
                }
            } else {

                if(cycle.contains(next)) {
                    for(DirectedEdge edge: cycle.getEdgesInCycle()) {
                        if(edge.equals(next))break;
                        partitionHalfEdges.get(edge).setDangle();
                    }
                    cycle = cycle.getSubCycle(next);
                }
                cycle.addToCycle(next);

                Face face = new Face(cycle);
                for (DirectedEdge edge : cycle.getEdgesInCycle()) {
                    partitionHalfEdges.get(edge).setIncidentFace(cycle);
                }
                if(face.getArea() > 0) partitionFaces.add(face);
            }

        }
    }

    private void GenIC() {

        Set<Cycle> incompleteCycles = new HashSet<>();
        Set<DirectedEdge> visited = new HashSet<>();

        for(HalfEdge startingHalfEdge: partitionHalfEdges.values()) {

            if(!startingHalfEdge.needsFace()) continue;

            Cycle cycle = new Cycle();
            cycle.addToCycle(startingHalfEdge.getEdge());

            DirectedEdge next = startingHalfEdge.getNext();

            while (next != null && !next.equals(startingHalfEdge.getEdge()) && !cycle.contains(next) && partitionHalfEdges.get(next).needsFace() && !visited.contains(next)) {
                cycle.addToCycle(next);
                next = partitionHalfEdges.get(next).getNext();
            }

            if (next == null && !partitionHalfEdges.get(cycle.getLastEdge()).isSpansMP()) {
                for(DirectedEdge edge: cycle.getEdgesInCycle()) {
                    partitionHalfEdges.get(edge).setDangle();
                }
            } else if(visited.contains(next) && next != null) {

                for(Cycle incompleteCycle : incompleteCycles) {
                    if(next.equals(incompleteCycle.getFirstEdge())) {

                        Cycle updatedCycle = new Cycle();
                        updatedCycle.addToRear(cycle);
                        updatedCycle.addToRear(incompleteCycle);

                        incompleteCycles.remove(incompleteCycle);
                        incompleteCycles.add(updatedCycle);
                        break;
                    }
                }

                visited.addAll(cycle.getEdgesInCycle());

            } else if (next == null) {
                visited.addAll(cycle.getEdgesInCycle());
                incompleteCycles.add(cycle);
            } else if(!partitionHalfEdges.get(next).needsFace()) {
                for(DirectedEdge edge: cycle.getEdgesInCycle()) {
                    partitionHalfEdges.get(edge).setDangle();
                }
            } else {

                if(cycle.contains(next)) {
                    for(DirectedEdge edge: cycle.getEdgesInCycle()) {
                        if(edge.equals(next))break;
                        partitionHalfEdges.get(edge).setDangle();
                    }
                    cycle = cycle.getSubCycle(next);
                }
                cycle.addToCycle(next);

                Face face = new Face(cycle);
                for (DirectedEdge edge : cycle.getEdgesInCycle()) {
                    partitionHalfEdges.get(edge).setIncidentFace(cycle);
                }
                if(face.getArea() > 0) partitionFaces.add(face);
            }

        }

        for(Cycle cycle: incompleteCycles) {
            if(cycle.size() > 1) partitionFaces.add(new Face(cycle, true));
        }
    }

    @Override
    public Iterator<GenOutput> call(Iterator<LineString> lineStringIterator) throws Exception {

        Set<GenOutput> result = new HashSet<>();

        GenOutput localDCEL = new GenOutput();
        Set<Face> completedFaces = new HashSet<>();
        Set<DDCELEntry> remPhaseInput = new HashSet<>();

        int partitionIndex = TaskContext.getPartitionId();
        boundary = partitionGrids.get(partitionIndex);
        long numberOfDangles;

        while (lineStringIterator.hasNext()) {
            LineString lineString = lineStringIterator.next();
            addVertex(lineString);
            addHalfEdge(lineString);
        }

        do {
            numberOfDangles = markCurrentDangles();
        } while (numberOfDangles > 0);

        for(Vertex vertex: partitionVertices.values()) {
            setNextAndPreviousForIncidentEdges(vertex);
        }

        markCutEdges();

        for(Vertex vertex: partitionVertices.values()) {
            if(updateVertex(vertex)) {
                setNextAndPreviousForIncidentEdges(vertex);
            }
        }

        if (remMethod == RemMethod.DDCEL_IC) {
            GenIC();
            for(Face face : partitionFaces) {
                if(face.hasIncompleteCycle()) {
                    remPhaseInput.add(face);
                }
            }
        } else {
            GenRH();
            for(HalfEdge halfEdge : partitionHalfEdges.values()) {
                if(halfEdge.needsFace()) {
                    remPhaseInput.add(halfEdge);
                }
            }
        }


        localDCEL.setVertices(partitionVertices.values());
        localDCEL.setHalfEdges(partitionHalfEdges.values());

        for(Face face : partitionFaces) {
            if(!face.hasIncompleteCycle()) {
                completedFaces.add(face);
            }
        }
        localDCEL.setFaces(completedFaces);
        localDCEL.setRemainingData(remPhaseInput);

        result.add(localDCEL);
        return result.iterator();
    }
}
