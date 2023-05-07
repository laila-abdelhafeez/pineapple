package org.apache.sedona.core.ddcel;

import org.apache.sedona.core.ddcel.entries.DDCELEntry;
import org.apache.sedona.core.ddcel.entries.Face;
import org.apache.sedona.core.ddcel.entries.HalfEdge;
import org.apache.sedona.core.ddcel.entries.Vertex;
import org.apache.sedona.core.ddcel.enums.RemMethod;
import org.apache.sedona.core.ddcel.enums.RepartitioningScheme;
import org.apache.sedona.core.ddcel.partitioning.DDCELEntryPartitioner;
import org.apache.sedona.core.ddcel.processing.output.GenOutput;
import org.apache.sedona.core.ddcel.processing.output.RemOutput;
import org.apache.sedona.core.ddcel.processing.GenPhase;
import org.apache.sedona.core.ddcel.processing.RemPhaseIC;
import org.apache.sedona.core.ddcel.processing.RemPhaseRH;
import org.apache.sedona.core.ddcel.utils.ConvertLinesToLineSegments;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

abstract public class DDCEL implements Serializable {

    protected SpatialRDD<LineString> lineSegmentsRDD = new SpatialRDD<>();

    private JavaRDD<Vertex> vertices;
    private JavaRDD<HalfEdge> halfEdges;
    private final List<JavaRDD<Face>> faces = new ArrayList<>();

    public DDCEL(SpatialRDD<LineString> spatialNetwork) {
        lineSegmentsRDD.rawSpatialRDD = spatialNetwork.rawSpatialRDD.flatMap(new ConvertLinesToLineSegments());
        lineSegmentsRDD.analyze();
    }

    protected void Gen(JavaRDD<GenOutput> partitionDCEL) {
        setVertices(partitionDCEL.flatMap((FlatMapFunction<GenOutput, Vertex>) dcel -> dcel.getVertices().iterator()));
        setHalfEdges(partitionDCEL.flatMap((FlatMapFunction<GenOutput, HalfEdge>) dcel -> dcel.getHalfEdges().iterator()));
        addFaceRDD(partitionDCEL.flatMap((FlatMapFunction<GenOutput, Face>) dcel -> dcel.getFaces().iterator()));
    }

    protected JavaRDD<DDCELEntry> Rem(JavaRDD<DDCELEntry> remPhaseInput, RemMethod remMethod){
        JavaRDD<RemOutput> remPhaseOutput;

        remPhaseInput = repartition(remPhaseInput, getPartitioner());
        if(remMethod == RemMethod.DDCEL_IC) {
            remPhaseOutput = remPhaseInput.mapPartitions(new RemPhaseIC(getPartitions()));
        } else {
            remPhaseOutput = remPhaseInput.mapPartitions(new RemPhaseRH(getPartitions()));
        }
        addFaceRDD(remPhaseOutput.flatMap((FlatMapFunction<RemOutput, Face>) remOutput -> remOutput.getFaces().iterator()));
        return remPhaseOutput.flatMap((FlatMapFunction<RemOutput, DDCELEntry>) remOutput -> remOutput.getRemainingData().iterator());
    }

    abstract protected void Rem(JavaRDD<DDCELEntry> remPhaseInput, RemMethod remMethod, RepartitioningScheme repartitioningScheme);

    abstract protected DDCELEntryPartitioner getPartitioner();

    abstract protected List<Envelope> getPartitions();

    protected void build(RemMethod remMethod, RepartitioningScheme repartitioningScheme) {
        JavaRDD<GenOutput> partitionDCEL = lineSegmentsRDD.spatialPartitionedRDD.mapPartitions(new GenPhase(getPartitions(), RemMethod.DDCEL_IC));
        Gen(partitionDCEL);
        JavaRDD<DDCELEntry> remPhaseInput = partitionDCEL.flatMap((FlatMapFunction<GenOutput, DDCELEntry>) dcel -> dcel.getRemainingData().iterator());
        Rem(remPhaseInput, remMethod, repartitioningScheme);
    }

    private JavaRDD<DDCELEntry> repartition(JavaRDD<DDCELEntry> remainingData, final DDCELEntryPartitioner partitioner) {
        return remainingData.flatMapToPair((PairFlatMapFunction<DDCELEntry, Integer, DDCELEntry>) partitioner::placeObject)
                .partitionBy(partitioner)
                .mapPartitions((FlatMapFunction<Iterator<Tuple2<Integer, DDCELEntry>>, DDCELEntry>) tuple2Iterator -> new Iterator<DDCELEntry>()
                {
                    @Override
                    public boolean hasNext()
                    {
                        return tuple2Iterator.hasNext();
                    }

                    @Override
                    public DDCELEntry next()
                    {
                        return tuple2Iterator.next()._2();
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                }, true);
    }

    protected List<Envelope> sample(double sampleFraction){
        return lineSegmentsRDD.rawSpatialRDD.sample(false, sampleFraction)
                .map((Function<LineString, Envelope>) Geometry::getEnvelopeInternal)
                .collect();
    }

    protected Envelope boundary() {
        return new Envelope( lineSegmentsRDD.boundaryEnvelope.getMinX(), lineSegmentsRDD.boundaryEnvelope.getMaxX() + 0.01,
                             lineSegmentsRDD.boundaryEnvelope.getMinY(), lineSegmentsRDD.boundaryEnvelope.getMaxY() + 0.01);
    }


    private void setVertices(JavaRDD<Vertex> vertices) {
        this.vertices = vertices;
    }
    public JavaRDD<Vertex> getVertices() {
        return vertices;
    }
    private void setHalfEdges(JavaRDD<HalfEdge> halfEdges) {
        this.halfEdges = halfEdges;
    }
    public JavaRDD<HalfEdge> getHalfEdges() {
        return halfEdges;
    }

    private void addFaceRDD(JavaRDD<Face> faces) {
        this.faces.add(faces);
    }
    public Iterator<JavaRDD<Face>> getFaces() {
        return faces.iterator();
    }

    public JavaRDD<Face> getFacesAt(int index) {
        if(index < faces.size()) return faces.get(index);
        return null;
    }

    public int j(){
        return faces.size();
    }

}
