package org.apache.sedona.core.ddcel;

import org.apache.sedona.core.ddcel.entries.DDCELEntry;
import org.apache.sedona.core.ddcel.enums.RemMethod;
import org.apache.sedona.core.ddcel.enums.RepartitioningScheme;
import org.apache.sedona.core.ddcel.partitioning.DDCELEntryPartitioner;
import org.apache.sedona.core.ddcel.partitioning.quadTree.DDCELQuadTree;
import org.apache.sedona.core.ddcel.partitioning.quadTree.DDCELQuadTreePartitioner;
import org.apache.sedona.core.ddcel.partitioning.quadTree.DDCELQuadtreePartitioning;
import org.apache.sedona.core.spatialPartitioning.QuadTreePartitioner;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.LineString;

import java.util.List;

public class QuadTreeDDCEL extends DDCEL {

    private DDCELQuadtreePartitioning treePartitioning;
    private DDCELQuadTree tree;

    public QuadTreeDDCEL(SpatialRDD<LineString> spatialNetwork, RemMethod remMethod, RepartitioningScheme repartitioningScheme,
                         double sampleFraction, int capacity, int maxTreeLevel, int minTreeLevel)
    throws Exception {
        super(spatialNetwork);
        partition(sample(sampleFraction), boundary(), capacity, maxTreeLevel, minTreeLevel);
        build(remMethod, repartitioningScheme);
    }

    public QuadTreeDDCEL(SpatialRDD<LineString> spatialNetwork, int capacity, int maxTreeLevel)
            throws Exception {
        super(spatialNetwork);
        partition(sample(0.2), boundary(), capacity, maxTreeLevel, -1);
        build(RemMethod.DDCEL_IC, RepartitioningScheme.MU);
    }

    private void partition(List<Envelope> samples, Envelope boundary, int capacity, int maxTreeLevel, int minTreeLevel)
    throws Exception {
        treePartitioning = new DDCELQuadtreePartitioning(samples, boundary, capacity, maxTreeLevel, minTreeLevel);
        tree = treePartitioning.getPartitionTree();
        lineSegmentsRDD.spatialPartitioning(new QuadTreePartitioner(tree));
    }

    @Override
    protected void Rem(JavaRDD<DDCELEntry> remPhaseInput, RemMethod remMethod, RepartitioningScheme repartitioningScheme) {
        switch (repartitioningScheme){
            case ONE_LU:
                while (!remPhaseInput.isEmpty() && treePartitioning.LevelUp())
                    remPhaseInput = Rem(remPhaseInput, remMethod);
                break;
            case TWO_LU:
                while (!remPhaseInput.isEmpty() && treePartitioning.TwoLevelsUp())
                    remPhaseInput = Rem(remPhaseInput, remMethod);
                break;
            case M1LU:
                treePartitioning.SkipToMiddle();
                while (!remPhaseInput.isEmpty() && treePartitioning.LevelUp())
                    remPhaseInput = Rem(remPhaseInput, remMethod);
                break;
            case MU:
                while (!remPhaseInput.isEmpty() && treePartitioning.MiddleUp())
                    remPhaseInput = Rem(remPhaseInput, remMethod);
                break;
        }
    }

    @Override
    protected DDCELEntryPartitioner getPartitioner() {
        return new DDCELQuadTreePartitioner(tree, treePartitioning.getCurrentLevel());
    }

    @Override
    protected List<Envelope> getPartitions() {
        return tree.fetchLeafZones(treePartitioning.getCurrentLevel());
    }


}
