package org.apache.sedona.core.ddcel.enums;

import java.io.Serializable;

public enum RemMethod implements Serializable {
    DDCEL_IC,
    DDCEL_RH;

    public static RemMethod getRemMethod(String str) {
        for (RemMethod me : RemMethod.values()) {
            if (me.name().equalsIgnoreCase(str)) { return me; }
        }
        return null;
    }

}
