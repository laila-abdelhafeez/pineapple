package org.apache.sedona.core.ddcel.processing.output;

import org.apache.sedona.core.ddcel.entries.DDCELEntry;
import org.apache.sedona.core.ddcel.entries.Face;

import java.io.Serializable;
import java.util.Collection;

public class RemOutput implements Serializable {
    private Collection<Face> faces;
    private Collection<DDCELEntry> remainingData;

    public Collection<Face> getFaces() {
        return faces;
    }

    public void setFaces(Collection<Face> faces) {
        this.faces = faces;
    }

    public Collection<DDCELEntry> getRemainingData() {
        return remainingData;
    }

    public void setRemainingData(Collection<DDCELEntry> remainingData) {
        this.remainingData = remainingData;
    }
}
