package org.apache.sedona.core.ddcel.processing;

import org.apache.sedona.core.ddcel.entries.*;
import org.apache.sedona.core.ddcel.enums.*;
import org.apache.sedona.core.ddcel.processing.output.RemOutput;
import org.apache.spark.TaskContext;
import org.locationtech.jts.geom.Envelope;

import java.util.*;

public class RemPhaseIC implements RemPhase {

    private final Map<DirectedEdge, Face> faceByFirstEdge = new HashMap<>();
    private final Map<Face, ConnectingStatus> connectingStatus = new HashMap<>();
    private final List<Envelope> partitionGrids;
    private Envelope boundary;

    public RemPhaseIC(List<Envelope> partitionGrids) {
        this.partitionGrids = partitionGrids;
    }

    private Face updateFace(Face face) {

        Face updatedFace = new Face(face.getOuterComponents(), face.hasIncompleteCycle());
        Set<Face> currentFaces = new HashSet<>();

        if(connectingStatus.containsKey(face)) {
            ConnectingStatus checkedFaceStatus = connectingStatus.get(face);
            if(checkedFaceStatus == ConnectingStatus.COMPLETED) return null; // already completed and added as part of a face, don't add again
            else if(checkedFaceStatus == ConnectingStatus.CANDIDATE) return face; // return parts of polygon that can form a polygon on other level
            else if(checkedFaceStatus == ConnectingStatus.CANT_BE_COMPLETED) return null; // this face can't be completed
            else return null; // should not be here
        }

        currentFaces.add(face);

        DirectedEdge lastEdge = face.getLastEdge();
        while (faceByFirstEdge.containsKey(lastEdge)) {

            Face found = faceByFirstEdge.get(lastEdge);

            if(connectingStatus.containsKey(found)) {
                ConnectingStatus checkedFaceStatus = connectingStatus.get(found);
                if(checkedFaceStatus == ConnectingStatus.COMPLETED) {
                    for(Face current: currentFaces) {
                        connectingStatus.putIfAbsent(current, ConnectingStatus.COMPLETED);
                    }
                    return null; // already completed and added as part of a face, don't add again
                }
                else if(checkedFaceStatus == ConnectingStatus.CANDIDATE) {
                    for(Face current: currentFaces) {
                        connectingStatus.putIfAbsent(current, ConnectingStatus.CANDIDATE);
                    }
                    return face; // return parts of polygon that can form a polygon on other level
                }
                else if(checkedFaceStatus == ConnectingStatus.CANT_BE_COMPLETED) {
                    for(Face current: currentFaces) {
                        connectingStatus.putIfAbsent(current, ConnectingStatus.CANT_BE_COMPLETED);
                    }
                    return null; // this face can't be completed
                }
                else return null; // should not be here
            }

            currentFaces.add(found);

            Cycle toAdd = found.getOuterComponents().removeFirst();
            updatedFace.addToRear(toAdd);

            if(updatedFace.getOuterComponents().createsFace()) {
                for(Face current: currentFaces) {
                    connectingStatus.putIfAbsent(current, ConnectingStatus.COMPLETED);
                }
                updatedFace.setCycleAsComplete();
                return updatedFace;
            }
            else if(updatedFace.getOuterComponents().hasInnerCycle()) {
                for(Face current: currentFaces) {
                    connectingStatus.putIfAbsent(current, ConnectingStatus.COMPLETED);
                }
                Cycle newCycle = updatedFace.getOuterComponents().getInnerCycle();
                updatedFace.updateFace(newCycle);
                updatedFace.setCycleAsComplete();
                return updatedFace;
            }
            lastEdge = updatedFace.getLastEdge();
        }

        if(boundary.contains(lastEdge.source) && boundary.contains(lastEdge.destination)) {
            for(Face current: currentFaces) {
                connectingStatus.putIfAbsent(current, ConnectingStatus.CANT_BE_COMPLETED);
            }
            return null;
        }

        for(Face current: currentFaces) {
            connectingStatus.putIfAbsent(current, ConnectingStatus.CANDIDATE);
        }
        return face;
    }


    @Override
    public Iterator<RemOutput> call(Iterator<DDCELEntry> remInputIterator) throws Exception {

        Set<RemOutput> result = new HashSet<>();
        RemOutput remOutput = new RemOutput();

        Set<Face> faces = new HashSet<>();
        Set<DDCELEntry> incompleteFaces = new HashSet<>();

        int partitionIndex = TaskContext.getPartitionId();
        boundary = partitionGrids.get(partitionIndex);

        while (remInputIterator.hasNext()) {
            Face face = (Face) remInputIterator.next();
            faceByFirstEdge.putIfAbsent(face.getFirstEdge(), face);
        }

        for(Face incompleteFace : faceByFirstEdge.values()) {
            Face updatedFace = updateFace(incompleteFace);
            if(updatedFace != null) {
                if(!updatedFace.hasIncompleteCycle() && updatedFace.getArea() > 0) faces.add(updatedFace);
                else if(updatedFace.hasIncompleteCycle()) incompleteFaces.add(updatedFace);
            }
        }

        remOutput.setFaces(faces);
        remOutput.setRemainingData(incompleteFaces);

        result.add(remOutput);
        return result.iterator();
    }

}
