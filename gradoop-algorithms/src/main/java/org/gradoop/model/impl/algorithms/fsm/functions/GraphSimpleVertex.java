package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.algorithms.fsm.tuples.SimpleVertex;
import org.gradoop.model.impl.id.GradoopId;

public class GraphSimpleVertex<V extends EPGMVertex> implements
  FlatMapFunction<V, Tuple2<GradoopId, SimpleVertex>> {

  @Override
  public void flatMap(V vertex,
    Collector<Tuple2<GradoopId, SimpleVertex>> collector) throws
    Exception {

    GradoopId vertexId = vertex.getId();
    String label = vertex.getLabel();

    for(GradoopId graphId : vertex.getGraphIds()) {
      collector.collect(
        new Tuple2<>(graphId, new SimpleVertex(vertexId, label)));
    }
  }
}
