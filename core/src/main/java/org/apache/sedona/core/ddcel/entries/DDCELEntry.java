package org.apache.sedona.core.ddcel.entries;

import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.List;

public interface DDCELEntry extends Serializable {
    Geometry getGeometry();
    List<Object> getParams();
}
