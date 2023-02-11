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

public class RtreeSGPAC implements SGPAC {

    STRtree dataIndex;
    AbstractNode root;
    Envelope boundary;

    public RtreeSGPAC(STRtree dataIndex) {
        this.dataIndex = dataIndex;
        root = dataIndex.getRoot();
        boundary = (Envelope) dataIndex.getRoot().getBounds();
    }

    public Set<Tuple2<String, Integer>> query(GeometryCollection polygonLayer, QueryMethod method) {

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
        }

        return results;
    }


    public Integer FR(Geometry queryPolygon) {
        Set<Geometry> result = new HashSet<>();
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
                return new HashSet<Geometry>(dataIndex.query(boundary)).size();
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
                return new HashSet<Geometry>(dataIndex.query(boundary)).size();
            case INTERSECT:
                if(!dataIndex.isEmpty()) {
                    Set<Geometry> result = new HashSet<>();
                    query(clippedPolygon, root, false, result);
                    return result.size();
                }
        }
        return 0;
    }

    public void query(Geometry queryPolygon, Object node, boolean within, Set<Geometry> result) {

        // node is a leaf node
        if(node instanceof ItemBoundable) {
            ItemBoundable itemBoundable = (ItemBoundable) node;
            Geometry item = (Geometry) itemBoundable.getItem();
            if(within) {
                result.add(item);
            } else {
                if(queryPolygon instanceof GeometryCollection) {
                    GeometryCollection query = (GeometryCollection) queryPolygon;
                    for (int i = 0; i < query.getNumGeometries(); ++i) {
                        try {
                            if(query.getGeometryN(i).contains(item) || query.getGeometryN(i).intersects(item)){
                                result.add(item);
                            }
                        } catch (Exception ignore) {}
                    }
                } else {
                    if(queryPolygon.contains(item) || queryPolygon.intersects(item)){
                        result.add(item);
                    }
                }
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
                    case INTERSECT :
                        for(Object child : currentNode.getChildBoundables()) {
                            query(clippedPolygon, child, false, result);
                        }
                        break;
                    default:
                        break;
                }
            }
        }

    }

}
