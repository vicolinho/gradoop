package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;

public class GraphVertices implements
  GroupReduceFunction<Tuple3<GradoopId, GradoopId, Integer>,
    Tuple2<GradoopId, ArrayList<Tuple2<GradoopId, Integer>>>> {

  @Override
  public void reduce(Iterable<Tuple3<GradoopId, GradoopId, Integer>> iterable,
    Collector<Tuple2<GradoopId, ArrayList<Tuple2<GradoopId, Integer>>>>
      collector) throws
    Exception {

    boolean first = true;
    GradoopId graphId = null;
    ArrayList<Tuple2<GradoopId, Integer>> vertices = Lists.newArrayList();

    for(Tuple3<GradoopId, GradoopId, Integer> vertex : iterable) {
      if(first) {
        graphId = vertex.f0;
        first = false;
      }
      vertices.add(new Tuple2<>(vertex.f1, vertex.f2));
    }
    collector.collect(new Tuple2<>(graphId, vertices));
  }
}
