package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.FatEdge;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;

public class GraphFatEdges implements
  GroupReduceFunction<Tuple3<GradoopId, FatEdge, CompressedDFSCode>,
  ArrayList<Tuple2<FatEdge, CompressedDFSCode>>> {

  @Override
  public void reduce(
    Iterable<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> iterable,
    Collector<ArrayList<Tuple2<FatEdge, CompressedDFSCode>>> collector) throws
    Exception {

    ArrayList<Tuple2<FatEdge, CompressedDFSCode>> graphFatEdges = Lists
      .newArrayList();

    for(Tuple3<GradoopId, FatEdge, CompressedDFSCode> fatEdge : iterable) {
      graphFatEdges.add(new Tuple2<>(fatEdge.f1, fatEdge.f2));
    }

    collector.collect(graphFatEdges);
  }
}
