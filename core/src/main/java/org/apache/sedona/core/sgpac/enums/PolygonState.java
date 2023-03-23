package org.apache.sedona.core.sgpac.enums;

/*
Possible results when checking polygon with an index node
 */

import java.io.Serializable;

public enum PolygonState implements Serializable {
    WITHIN,
    INTERSECT,
    OUTSIDE
}
