package org.apache.sedona.core.sgpac.RTree;

import org.apache.sedona.core.sgpac.PolygonClip;
import org.apache.sedona.core.sgpac.SGPAC;
import org.apache.sedona.core.sgpac.enums.PolygonState;
import org.apache.sedona.core.sgpac.enums.QueryMethod;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.strtree.AbstractNode;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RtreeSGPAC extends SGPAC {
    AbstractNode root;

    public RtreeSGPAC(STRtree dataIndex) {
        super(dataIndex);
        root = dataIndex.getRoot();
    }

    public RtreeSGPAC(STRtree dataIndex, int estimatorCellCount) {
        super(dataIndex, estimatorCellCount);
        root = dataIndex.getRoot();
    }

    public void query(Geometry queryPolygon, List<Geometry> result) {
        STRtree tree = (STRtree) dataIndex;
        if(!tree.isEmpty()) query(queryPolygon, root, false, result);
    }

    public void query(Geometry queryPolygon, Object node, boolean within, List<Geometry> result) {

        // node is a leaf node
        if(node instanceof ItemBoundable) {
            ItemBoundable itemBoundable = (ItemBoundable) node;
            Geometry item = (Geometry) itemBoundable.getItem();
            if(within) {
                result.add(item);
            } else if(queryPolygon.contains(item) || queryPolygon.intersects(item)){
                result.add(item);
            }
        }

        // node is a non-leaf node
        if(node instanceof AbstractNode) {
            AbstractNode currentNode = (AbstractNode) node;
            if(within) {
                List childBoundables = currentNode.getChildBoundables();
                for(Object child : childBoundables) {
                    query(queryPolygon, child, true, result);
                }
            } else {
                Tuple2<PolygonState, Geometry> partitionClip = PolygonClip.clip((Envelope) currentNode.getBounds(), queryPolygon);
                PolygonState state = partitionClip._1();
                Geometry clippedPolygon = partitionClip._2();

                switch (state) {
                    case WITHIN:
                        for(Object child : currentNode.getChildBoundables()) {
                            query(clippedPolygon, child, true, result);
                        }
                        break;
                    case INTERSECT:
                        for(Object child : currentNode.getChildBoundables()) {
                            query(clippedPolygon, child, false, result);
                        }
                        break;
                    case OUTSIDE:
                        break;
                }
            }
        }

    }

    protected double estimatePolygon(Geometry geometry, QueryMethod method) {

        double candidateSet;
        double n;
        double operationsCount;

        switch (method) {
            case FR: {
                candidateSet = getCountInMBR(geometry.getEnvelopeInternal())._1;
                n = geometry.getNumPoints();
                operationsCount = candidateSet*n;
                return operationsCount;
            }

            case SGPAC_2L: {
                Tuple2<Integer, Integer> countIntersectTuple = getCountInMBR(geometry.getEnvelopeInternal());
                n = geometry.getNumPoints();
                candidateSet = countIntersectTuple._1;
                int cellCount = countIntersectTuple._2;

                STRtree tree = (STRtree) dataIndex;

                int factorMedDepth = (int)Math.pow(tree.getNodeCapacity(), tree.depth()/2.0);
                double intersect = cellCount*(n/factorMedDepth)*Math.log10(n/factorMedDepth);

                double modifiedN = n/factorMedDepth;
                operationsCount = modifiedN*candidateSet + intersect;
                return operationsCount;
            }

            default:
                return 0.0;

        }
    }

    protected Set<Envelope> getIntersectingCells(Envelope queryMBR) {
        Set<Envelope> result = new HashSet<>();
        if(queryMBR.contains(boundary)) {
            result.add(boundary);
        } else if(queryMBR.intersects(boundary)) {
            List children = root.getChildBoundables();
            for(Object child : children) getIntersectingCells(child, queryMBR, result);
        }
        return result;
    }

    private void getIntersectingCells(Object node, Envelope queryMBR, Set<Envelope> cells) {
        if(node instanceof AbstractNode)  {
            AbstractNode currentNode = (AbstractNode) node;
            Envelope currentEnvelope = (Envelope)currentNode.getBounds();

            if(queryMBR.contains(currentEnvelope)) {
                cells.add(currentEnvelope);
            } else if(queryMBR.intersects(currentEnvelope)) {
                cells.add(currentEnvelope);
                List children = currentNode.getChildBoundables();
                for(Object child : children) getIntersectingCells(child, queryMBR, cells);
            }
        }
    }

}
