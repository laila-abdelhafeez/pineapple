package org.apache.sedona.core.sgpac;

import org.apache.sedona.core.sgpac.enums.QueryMethod;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

public class SGPACQuery {


    public static <U extends Geometry, T extends Geometry> JavaPairRDD<String, Integer> SGPAC_2L
            (SpatialRDD<T> data, SpatialRDD<U> polygonLayer) {

        return data.indexedRDD.zipPartitions(polygonLayer.spatialPartitionedRDD, new SGPACPartition<U>(QueryMethod.SGPAC_2L))
                .mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerPair) throws Exception {
                        return stringIntegerPair;
                    }
                }).aggregateByKey(0, (Function2<Integer, Integer, Integer>) Integer::sum, (Function2<Integer, Integer, Integer>) Integer::sum);

    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<String, Integer> SGPAC_1L
            (SpatialRDD<T> data, SpatialRDD<U> polygonLayer) {

        return data.indexedRDD.zipPartitions(polygonLayer.spatialPartitionedRDD, new SGPACPartition<U>(QueryMethod.SGPAC_1L))
                .mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerPair) throws Exception {
                        return stringIntegerPair;
                    }
                }).aggregateByKey(0, (Function2<Integer, Integer, Integer>) Integer::sum, (Function2<Integer, Integer, Integer>) Integer::sum);

    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<String, Integer> SGPAC_QO
            (SpatialRDD<T> data, SpatialRDD<U> polygonLayer, int estimatorCellCount) {

        return data.indexedRDD.zipPartitions(polygonLayer.spatialPartitionedRDD, new SGPACPartition<U>(estimatorCellCount))
                .mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerPair) throws Exception {
                        return stringIntegerPair;
                    }
                }).aggregateByKey(0, (Function2<Integer, Integer, Integer>) Integer::sum, (Function2<Integer, Integer, Integer>) Integer::sum);

    }


}