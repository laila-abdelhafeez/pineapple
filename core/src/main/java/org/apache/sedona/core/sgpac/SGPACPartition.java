package org.apache.sedona.core.sgpac;

import org.apache.commons.lang.NotImplementedException;
import org.apache.sedona.core.sgpac.RTree.RtreeSGPAC;
import org.apache.sedona.core.sgpac.RTree.RtreeSGPACQO;
import org.apache.sedona.core.sgpac.enums.QueryMethod;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;


public class SGPACPartition<U extends Geometry>
        implements Serializable,
                   FlatMapFunction2<Iterator<SpatialIndex>, Iterator<U>, Tuple2<String, Integer>>
{

    QueryMethod queryMethod;
    SGPAC sgpacQuery;
    int estimatorCellCount = 10;

    List<Envelope> partitionGrids;

    public SGPACPartition(QueryMethod queryOption, List<Envelope> partitionGrids) {
        this.queryMethod = queryOption;
        this.partitionGrids = partitionGrids;
    }

    public SGPACPartition(int estimatorCellCount, List<Envelope> partitionGrids) {
        this.queryMethod = QueryMethod.SGPAC_QO;
        this.estimatorCellCount = estimatorCellCount;
        this.partitionGrids = partitionGrids;
    }

    /*
      The call function is invoked for each data partition.
      Input: partition local index of the data as an input and the partition set of polygons
      Output: the partition polygon name/id and the data within the polygon count 
     */

    public Iterator<Tuple2<String, Integer>> call(Iterator<SpatialIndex> dataIndexIterator, Iterator<U> polygonLayerIterator) throws Exception {

        int partitionIndex = TaskContext.getPartitionId();

        Set<Tuple2<String, Integer>> result = new HashSet<>();

        if (dataIndexIterator.hasNext() && polygonLayerIterator.hasNext()) {
            SpatialIndex dataIndex = dataIndexIterator.next();

            Geometry[] geometries;
            ArrayList<Geometry> geometryArrayList = new ArrayList<>();

            while (polygonLayerIterator.hasNext()) {
                Geometry polygon = polygonLayerIterator.next();
                geometryArrayList.add(polygon);
            }

            geometries = new Geometry[geometryArrayList.size()];
            for(int i = 0; i < geometryArrayList.size(); ++i) {
                geometries[i] = geometryArrayList.get(i);
            }

            GeometryCollection polygonLayer = new GeometryCollection(geometries, new GeometryFactory());

            if(dataIndex instanceof STRtree) {

                STRtree rtreeIndex = (STRtree) dataIndex;

                if(queryMethod == QueryMethod.SGPAC_QO) {
                    sgpacQuery =  new RtreeSGPACQO(rtreeIndex, estimatorCellCount);
                } else {
                    sgpacQuery = new RtreeSGPAC(rtreeIndex);
                }
                sgpacQuery.setBoundary(partitionGrids.get(partitionIndex));
                result = sgpacQuery.query(polygonLayer, queryMethod);


            } else if (dataIndex instanceof Quadtree) {
                throw new NotImplementedException();
            }


        }

        return result.iterator();
    }
}
