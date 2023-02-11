package org.apache.sedona.core.sgpac.enums;

/*
 SGPAC_2L: 2 level polygon clipping by global and local indexes
 SGPAC_1L: 1 level polygon clipping by the global index
 SGPAC_QO: Query Optimizer choose between traditional FR and SGPAC_2L
 FR: traditional filter-refine approach
 */

public enum QueryMethod {
    SGPAC_2L,
    SGPAC_1L,
    SGPAC_QO,
    FR
}
