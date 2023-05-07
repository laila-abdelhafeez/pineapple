package org.apache.sedona.core.ddcel.partitioning.quadTree;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.sedona.core.ddcel.entries.DDCELEntry;
import org.apache.sedona.core.ddcel.entries.DirectedEdge;
import org.apache.sedona.core.ddcel.entries.Face;
import org.apache.sedona.core.ddcel.entries.HalfEdge;
import org.apache.sedona.core.spatialPartitioning.quadtree.QuadRectangle;
import org.apache.sedona.core.spatialPartitioning.quadtree.StandardQuadTree;
import org.locationtech.jts.geom.Envelope;
import scala.Tuple2;

import java.util.*;

public class DDCELQuadTree<T> extends StandardQuadTree<T> {

    public DDCELQuadTree(QuadRectangle definition, int level) {
        super(definition, level);
    }

    public DDCELQuadTree(QuadRectangle definition, int level, int maxItemsPerZone, int maxLevel) {
        super(definition, level, maxItemsPerZone, maxLevel);
    }


    public List<QuadRectangle> findZones(int level, Envelope envelope) {

        if(level == -1) return findZones(new QuadRectangle(envelope));

        final List<QuadRectangle> matches = new ArrayList<>();

        traverse(new Visitor<T>() {
            @Override
            public boolean visit(StandardQuadTree<T> tree) {
                if (!disjoint(tree.getZone().getEnvelope(), envelope)){
                    if(tree.getLevel() == level || tree.isLeaf()) {
                        matches.add(tree.getZone());
                        return false;
                    }
                    return true;

                } else {
                    return false;
                }
            }});

        return matches;
    }

    public List<Envelope> fetchLeafZones(int level) {

        if(level == -1) return fetchLeafZones();

        final List<Envelope> leafZones = new ArrayList<>();
        traverse(new Visitor<T>()
        {
            @Override
            public boolean visit(StandardQuadTree<T> tree)
            {
                if (tree.getLevel() == level || tree.isLeaf()) {
                    leafZones.add(tree.getZone().getEnvelope());
                    return false;
                }
                return true;
            }
        });

        return leafZones;
    }

    public void assignPartitionIds(int level) {
        traverse(new Visitor<T>()
        {
            private int partitionId = 0;

            @Override
            public boolean visit(StandardQuadTree<T> tree)
            {
                if (tree.getLevel() == level || tree.isLeaf()) {
                    tree.getZone().partitionId = partitionId;
                    partitionId++;
                    return false;
                }
                return true;
            }
        });
    }

    public int getMaxLevel() {
        final MutableInt maxLevel = new MutableInt(0);

        traverse(new Visitor<T>()
        {
            @Override
            public boolean visit(StandardQuadTree<T> tree)
            {
                if (tree.isLeaf() && tree.getLevel() > maxLevel.toInteger()) {
                    maxLevel.setValue(tree.getLevel());
                }
                return true;
            }
        });

        return maxLevel.toInteger();
    }


    public Iterator<Tuple2<Integer, DDCELEntry>> placeDDCELEntry(DDCELEntry entry, int level) {

        final Set<Tuple2<Integer, DDCELEntry>> result = new HashSet<>();
        List<QuadRectangle> matchedPartitions;
        if(entry instanceof HalfEdge) {
            HalfEdge halfEdge = (HalfEdge) entry;
            matchedPartitions = findZones(level, halfEdge.getEdge().toLineString().getEnvelopeInternal());

        } else {
            assert entry instanceof Face;
            Face face = (Face) entry;
            matchedPartitions = findZones(level, face.getFirstEdge().toLineString().getEnvelopeInternal());

            List<DirectedEdge> edges = face.getOuterComponents().getEdgesInCycle();
            for (int i = 1; i < edges.size(); ++i) {
                List<QuadRectangle> newMatchedPartitions = findZones(level, edges.get(i).toLineString().getEnvelopeInternal());
                matchedPartitions.retainAll(newMatchedPartitions);
            }
        }

        assert matchedPartitions.size() > 0;
        for (QuadRectangle partition : matchedPartitions) {
            result.add(new Tuple2<>(partition.partitionId, entry));
        }

        return result.iterator();
    }

}
