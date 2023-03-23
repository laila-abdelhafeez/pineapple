package org.apache.sedona.core.sgpac;

import org.apache.spark.api.java.function.FlatMapFunction2;
import org.locationtech.jts.geom.Geometry;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

public class SGPACJoin<T extends Geometry, U extends Geometry> implements Serializable, FlatMapFunction2<Iterator<T>, Iterator<U>, Tuple2<String, Integer>> {

    @Override
    public Iterator<Tuple2<String, Integer>> call(Iterator<T> objectsIterator, Iterator<U> polygonsIterator) throws Exception {

        Set<Tuple2<String, Integer>> results = new HashSet<>();
        List<Geometry> objects = new ArrayList<>();
        while (objectsIterator.hasNext()) objects.add(objectsIterator.next());


        while (polygonsIterator.hasNext()) {
            Geometry polygon = polygonsIterator.next();
            int count = 0;
            for(Geometry object : objects) {
                if(!polygon.disjoint(object)) count++;
            }
            results.add(new Tuple2<>(polygon.getUserData().toString(), count));
        }

        return results.iterator();
    }
}
