package org.apache.sedona.core.sgpac.RTree;

import org.apache.sedona.core.sgpac.enums.QueryMethod;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.index.strtree.AbstractNode;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RtreeSGPACQO extends RtreeSGPAC {

    private boolean estimatorReady = false;
    private final int estimatorCellCount;
    double estimatorWidthPerCell, estimatorHeightPerCell;
    private final HashMap<Tuple2<Integer, Integer>, Tuple2<Integer, Set<Envelope>>> estimator = new HashMap<>();

    public RtreeSGPACQO(STRtree dataIndex, int estimatorCellCount) {
        super(dataIndex);
        this.estimatorCellCount = estimatorCellCount;
    }

    public Set<Tuple2<String, Integer>> query(GeometryCollection polygonLayer, QueryMethod ignore) {
        int numberOfGeometries = polygonLayer.getNumGeometries();

        buildEstimator();
        double frEstimate = 0;
        double sgpac2LEstimate = 0;
        for (int i = 0; i < numberOfGeometries; ++i) {
            Geometry geometry = polygonLayer.getGeometryN(i);
            frEstimate += estimatePolygon(geometry, QueryMethod.FR);
            sgpac2LEstimate += estimatePolygon(geometry, QueryMethod.SGPAC_2L);
        }
        if(frEstimate < sgpac2LEstimate) return super.query(polygonLayer, QueryMethod.FR);
        else return super.query(polygonLayer, QueryMethod.SGPAC_2L);
    }

    private double estimatePolygon(Geometry geometry, QueryMethod method) {

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

                int factorMedDepth = (int)Math.pow(dataIndex.getNodeCapacity(), dataIndex.depth()/2.0);
                double intersect = cellCount*(n/factorMedDepth)*Math.log10(n/factorMedDepth);

                double modifiedN = n/factorMedDepth;
                operationsCount = modifiedN*candidateSet + intersect;
                return operationsCount;
            }

            default:
                return 0.0;

        }
    }

    private Tuple2<Integer, Integer> getCountInMBR(Envelope MBR) {
        double count = 0;
        Set<Envelope> cells = new HashSet<>();

        int xStartIndex = (int) Math.round((MBR.getMinX() - boundary.getMinX()) / estimatorWidthPerCell);
        int yStartIndex = (int) Math.round((MBR.getMinY() - boundary.getMinY()) / estimatorHeightPerCell);

        int xEndIndex = (int) Math.round((MBR.getMaxX() - boundary.getMinX()) / estimatorWidthPerCell);
        int yEndIndex = (int) Math.round((MBR.getMaxY() - boundary.getMinY()) / estimatorHeightPerCell);

        for(int i = xStartIndex; i <= xEndIndex; i++){
            for(int j = yStartIndex; j <= yEndIndex; j++){
                Tuple2<Integer, Set<Envelope>> current = estimator.getOrDefault(new Tuple2<>(i, j), new Tuple2<>(0, new HashSet<>()));
                count += current._1();
                cells.addAll(current._2());
            }
        }
        return new Tuple2<>((int)count, cells.size());
    }

    public void buildEstimator() {

        if(estimatorReady)return;

        estimatorWidthPerCell = boundary.getWidth()/ estimatorCellCount;
        estimatorHeightPerCell = boundary.getHeight()/ estimatorCellCount;

        for(int x = 0; x < estimatorCellCount; ++x) {
            for(int y = 0; y < estimatorCellCount; ++y) {
                Envelope envelope = new Envelope(boundary.getMinX() + x * estimatorWidthPerCell,
                        boundary.getMinX() + (x + 1) * estimatorWidthPerCell,
                        boundary.getMinY() + y * estimatorHeightPerCell,
                        boundary.getMinY() + (y + 1) * estimatorHeightPerCell);

                List<Geometry> candidate = dataIndex.query(envelope);
                int count = candidate.size();
                estimator.put(new Tuple2<>(x, y), new Tuple2<>(count, getIntersectingCells(envelope)));
            }
        }
        estimatorReady = true;
    }

    public Set<Envelope> getIntersectingCells(Envelope queryMBR) {
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
