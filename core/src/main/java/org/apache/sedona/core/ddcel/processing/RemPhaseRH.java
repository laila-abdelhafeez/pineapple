package org.apache.sedona.core.ddcel.processing;

import org.apache.sedona.core.ddcel.entries.*;
import org.apache.sedona.core.ddcel.processing.output.RemOutput;
import org.apache.spark.TaskContext;
import org.locationtech.jts.geom.Envelope;


import java.util.*;

public class RemPhaseRH implements RemPhase{

    Map<DirectedEdge, HalfEdge> remainingHalfEdges = new HashMap<>();
    Set<Face> partitionFaces = new HashSet<>();
    private final List<Envelope> partitionGrids;
    private Envelope boundary;

    public RemPhaseRH(List<Envelope> partitionGrids) {
        this.partitionGrids = partitionGrids;
    }

    private void setLocalFaces() {

        Set<DirectedEdge> visited = new HashSet<>();

        for(HalfEdge startingHalfEdge: remainingHalfEdges.values()) {

            if(!startingHalfEdge.needsFace()) continue;

            Cycle cycle = new Cycle();
            cycle.addToCycle(startingHalfEdge.getEdge());

            DirectedEdge next = startingHalfEdge.getNext();

            while (next != null && remainingHalfEdges.containsKey(next) && !next.equals(startingHalfEdge.getEdge()) && !cycle.contains(next) && remainingHalfEdges.get(next).needsFace() && !visited.contains(next)) {
                cycle.addToCycle(next);
                next = remainingHalfEdges.get(next).getNext();
            }

            if (next == null) {
                for(DirectedEdge edge: cycle.getEdgesInCycle()) {
                    remainingHalfEdges.get(edge).setDangle();
                }
            } else if(!remainingHalfEdges.containsKey(next)) {

                DirectedEdge lastCycleEdge = cycle.getLastEdge();
                if(boundary.contains(lastCycleEdge.source) && boundary.contains(lastCycleEdge.destination)) {
                    for(DirectedEdge edge: cycle.getEdgesInCycle()) {
                        remainingHalfEdges.get(edge).setDangle();
                    }
                } else {
                    visited.addAll(cycle.getEdgesInCycle());
                }

            } else if(visited.contains(next)) {
                visited.addAll(cycle.getEdgesInCycle());
            } else if(!remainingHalfEdges.get(next).needsFace()) {
                for(DirectedEdge edge: cycle.getEdgesInCycle()) {
                    remainingHalfEdges.get(edge).setDangle();
                }

            } else {

                if(cycle.contains(next)) {
                    for(DirectedEdge edge: cycle.getEdgesInCycle()) {
                        if(edge.equals(next))break;
                        remainingHalfEdges.get(edge).setDangle();
                    }
                    cycle = cycle.getSubCycle(next);
                }
                cycle.addToCycle(next);

                Face face = new Face(cycle);
                for (DirectedEdge edge : cycle.getEdgesInCycle()) {
                    remainingHalfEdges.get(edge).setIncidentFace(cycle);
                }
                if(face.getArea() > 0) partitionFaces.add(face);
            }

        }
    }

    @Override
    public Iterator<RemOutput> call(Iterator<DDCELEntry> remInputIterator) throws Exception {

        int partitionIndex = TaskContext.getPartitionId();
        boundary = partitionGrids.get(partitionIndex);

        RemOutput remOutput = new RemOutput();

        Set<RemOutput> result = new HashSet<>();
        Set<DDCELEntry> remOutputRH = new HashSet<>();

        while (remInputIterator.hasNext()) {
            HalfEdge halfEdge = (HalfEdge) remInputIterator.next();
            if(remainingHalfEdges.containsKey(halfEdge.getEdge())) {
                HalfEdge existingHalfEdge = remainingHalfEdges.get(halfEdge.getEdge());
                if(existingHalfEdge.getNext() == null && halfEdge.getNext() != null){
                    remainingHalfEdges.replace(halfEdge.getEdge(), existingHalfEdge, halfEdge);
                }
            } else {
                if(halfEdge.needsFace()) remainingHalfEdges.put(halfEdge.getEdge(), halfEdge);
            }
        }

        setLocalFaces();
        for(HalfEdge halfEdge : remainingHalfEdges.values()) {
            if(halfEdge.needsFace()) remOutputRH.add(halfEdge);
        }

        remOutput.setFaces(partitionFaces);
        remOutput.setRemainingData(remOutputRH);

        result.add(remOutput);

        return result.iterator();
    }
}
