package org.apache.sedona.core.ddcel.partitioning.quadTree;

import org.apache.sedona.core.spatialPartitioning.quadtree.QuadRectangle;
import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.util.List;

public class DDCELQuadtreePartitioning implements Serializable {

    private int currentLevel = 0;
    private final DDCELQuadTree<Integer> dcelPartitionTree;

    public DDCELQuadtreePartitioning(List<Envelope> samples, Envelope boundary, int capacity, int maxTreeLevel)
            throws Exception {
        this(samples, boundary, capacity, maxTreeLevel, -1);
    }

    public DDCELQuadtreePartitioning(List<Envelope> samples, Envelope boundary, int capacity, int maxTreeLevel, int minTreeLevel)
            throws Exception {

        dcelPartitionTree = new DDCELQuadTree<>(new QuadRectangle(boundary), 0, capacity, maxTreeLevel);
        if (minTreeLevel > 0) {
            dcelPartitionTree.forceGrowUp(minTreeLevel);
        }

        for (final Envelope sample : samples) {
            dcelPartitionTree.insert(new QuadRectangle(sample), 1);
        }

        resetCurrentLevel();
    }

    public DDCELQuadTree<Integer> getPartitionTree()
    {
        return dcelPartitionTree;
    }


    public void resetCurrentLevel() {
        currentLevel = dcelPartitionTree.getMaxLevel();
        dcelPartitionTree.assignPartitionIds();
    }

    public void SkipToMiddle() {
        currentLevel /= 2;
    }

    public boolean MiddleUp(){
        if(currentLevel <= 0) return false;
        if(currentLevel == 1) return goToRoot();
        currentLevel /= 2;
        dcelPartitionTree.assignPartitionIds(currentLevel);
        return true;
    }

    public boolean LevelUp() {
        currentLevel -= 1;
        if(currentLevel < 0) return false;
        dcelPartitionTree.assignPartitionIds(currentLevel);
        return true;
    }

    public boolean TwoLevelsUp() {
        currentLevel -= 1;
        if(currentLevel == 0) return goToRoot();
        return LevelUp();
    }

    public boolean goToRoot() {
        currentLevel = 0;
        dcelPartitionTree.assignPartitionIds(currentLevel);
        return true;
    }

    public int getCurrentLevel() {
        return currentLevel;
    }
}
