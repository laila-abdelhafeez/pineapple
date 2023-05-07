package org.apache.sedona.core.ddcel.enums;

import java.io.Serializable;

public enum RepartitioningScheme implements Serializable {
    ONE_LU,
    TWO_LU,
    M1LU,
    MU;

    public static RepartitioningScheme getRepartitioningScheme(String str) {
        for (RepartitioningScheme me : RepartitioningScheme.values()) {
            if (me.name().equalsIgnoreCase(str)) { return me; }
        }
        return null;
    }
}
