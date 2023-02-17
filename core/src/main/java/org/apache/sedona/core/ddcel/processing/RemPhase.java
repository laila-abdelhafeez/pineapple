package org.apache.sedona.core.ddcel.processing;

import org.apache.sedona.core.ddcel.entries.DDCELEntry;
import org.apache.sedona.core.ddcel.processing.output.RemOutput;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;

public interface RemPhase extends FlatMapFunction<Iterator<DDCELEntry>, RemOutput> { }
