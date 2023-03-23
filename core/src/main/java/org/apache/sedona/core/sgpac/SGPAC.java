package org.apache.sedona.core.sgpac;

import org.apache.sedona.core.sgpac.enums.QueryMethod;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryCollection;
import scala.Serializable;
import scala.Tuple2;

import java.util.Set;

public interface SGPAC extends Serializable {
    public Set<Tuple2<String, Integer>> query(GeometryCollection polygonLayer, QueryMethod method);

    public void setBoundary(Envelope envelope);

}
