package org.apache.sedona.core.ddcel.partitioning.quadTree;

import org.apache.sedona.core.ddcel.entries.DDCELEntry;
import org.apache.sedona.core.ddcel.partitioning.DDCELEntryPartitioner;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.Iterator;

public class DDCELQuadTreePartitioner
        extends DDCELEntryPartitioner
{
    private final DDCELQuadTree<? extends Geometry> quadTree;
    private final int partitionLevel;

    public DDCELQuadTreePartitioner(DDCELQuadTree<? extends Geometry> quadTree, int partitionLevel)
    {
        super(GridType.QUADTREE, quadTree.fetchLeafZones(partitionLevel));
        this.quadTree = quadTree;
        this.partitionLevel = partitionLevel;
        this.quadTree.dropElements();
    }


    @Override
    public Iterator<Tuple2<Integer, DDCELEntry>> placeObject(DDCELEntry entry) throws Exception {
        return quadTree.placeDDCELEntry(entry, partitionLevel);
    }

    @Nullable
    @Override
    public DedupParams getDedupParams()
    {
        return new DedupParams(grids);
    }

    @Override
    public int numPartitions()
    {
        return grids.size();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof DDCELQuadTreePartitioner)) {
            return false;
        }

        final DDCELQuadTreePartitioner other = (DDCELQuadTreePartitioner) o;
        return other.quadTree.equals(this.quadTree);
    }
}
