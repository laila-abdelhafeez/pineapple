package org.apache.sedona.core.sgpac;

import org.apache.sedona.core.sgpac.enums.PolygonState;
import org.apache.sedona.core.sgpac.enums.QueryMethod;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

public abstract class SGPAC implements Serializable {

    protected SpatialIndex dataIndex;
    protected Envelope boundary;

    protected boolean estimatorReady = false;
    protected final int estimatorCellCount;
    protected double estimatorWidthPerCell, estimatorHeightPerCell;
    protected final HashMap<Tuple2<Integer, Integer>, Tuple2<Integer, Set<Envelope>>> estimator = new HashMap<>();


    public SGPAC(SpatialIndex dataIndex, int estimatorCellCount) {
        this.dataIndex = dataIndex;
        this.estimatorCellCount = estimatorCellCount;
    }

    public SGPAC(SpatialIndex dataIndex) {
        this.dataIndex = dataIndex;
        this.estimatorCellCount = 0;
    }

    public Set<Tuple2<String, Integer>> query(GeometryCollection polygonLayer, QueryMethod method){
        Set<Tuple2<String, Integer>> results = new HashSet<Tuple2<String, Integer>>();
        int numberOfGeometries = polygonLayer.getNumGeometries();

        switch (method) {
            case FR:
                for(int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    String geometryName = geometry.getUserData().toString();
                    int size = FR(geometry);
                    results.add(new Tuple2<>(geometryName, size));
                }
                break;

            case SGPAC_1L:
                for(int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    String geometryName = geometry.getUserData().toString();
                    int size = SGPAC_1L(geometry);
                    results.add(new Tuple2<>(geometryName, size));
                }
                break;

            case SGPAC_2L:
                for(int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    String geometryName = geometry.getUserData().toString();
                    int size = SGPAC_2L(geometry);
                    results.add(new Tuple2<>(geometryName, size));
                }
                break;

            case SGPAC_QO:
                buildEstimator();
                double frEstimate = 0;
                double sgpac2LEstimate = 0;
                for (int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    frEstimate += estimatePolygon(geometry, QueryMethod.FR);
                    sgpac2LEstimate += estimatePolygon(geometry, QueryMethod.SGPAC_2L);
                }
                if(frEstimate < sgpac2LEstimate) query(polygonLayer, QueryMethod.FR);
                else query(polygonLayer, QueryMethod.SGPAC_2L);
                break;
        }

        return results;
    }

    public void setBoundary(Envelope envelope) {
        boundary = envelope;
    }

    public Integer FR(Geometry queryPolygon) {
        List<Geometry> result = new ArrayList<>();
        List<Geometry> candidateSet = dataIndex.query(queryPolygon.getEnvelopeInternal());
        for (Geometry candidateItem : candidateSet) {
            if(queryPolygon.contains(candidateItem) || queryPolygon.intersects(candidateItem)) {
                result.add(candidateItem);
            }
        }
        return result.size();
    }

    public Integer SGPAC_1L(Geometry queryPolygon) {
        Tuple2<PolygonState, Geometry> partitionClip = PolygonClip.clip(boundary, queryPolygon);
        PolygonState state = partitionClip._1();
        Geometry clippedPolygon = partitionClip._2();

        switch (state) {
            case WITHIN:
                return dataIndex.query(boundary).size();
            case INTERSECT:
                return FR(clippedPolygon);
        }
        return 0;
    }

    public Integer SGPAC_2L(Geometry queryPolygon) {

        Tuple2<PolygonState, Geometry> partitionClip = PolygonClip.clip(boundary, queryPolygon);
        PolygonState state = partitionClip._1();
        Geometry clippedPolygon = partitionClip._2();

        switch (state) {
            case WITHIN:
                return dataIndex.query(boundary).size();
            case INTERSECT:
                List<Geometry> result = new ArrayList<>();
                query(clippedPolygon, result);
                return result.size();

        }
        return 0;
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

    protected Tuple2<Integer, Integer> getCountInMBR(Envelope MBR) {
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

    abstract public void query(Geometry queryPolygon, List<Geometry> result);
    abstract protected double estimatePolygon(Geometry geometry, QueryMethod method);
    abstract protected Set<Envelope> getIntersectingCells(Envelope queryMBR);
}
