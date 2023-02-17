package org.apache.sedona.core.ddcel.partitioning;

import org.apache.sedona.core.ddcel.entries.DDCELEntry;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.spark.Partitioner;
import org.locationtech.jts.geom.Envelope;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

abstract public class DDCELEntryPartitioner extends Partitioner implements Serializable {

    protected final GridType gridType;
    protected final List<Envelope> grids;

    protected DDCELEntryPartitioner(GridType gridType, List<Envelope> grids)
    {
        this.gridType = gridType;
        this.grids = Objects.requireNonNull(grids, "grids");
    }

    /**
     * Given a ddcel half-edge/face, returns a list of partitions it overlaps.
     */
    abstract public Iterator<Tuple2<Integer, DDCELEntry>>
    placeObject(DDCELEntry entry)
            throws Exception;

    @Nullable
    abstract public DedupParams getDedupParams();

    public GridType getGridType()
    {
        return gridType;
    }

    public List<Envelope> getGrids()
    {
        return grids;
    }

    @Override
    public int getPartition(Object key)
    {
        return (int) key;
    }
}
