package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.SimpleEdge;
import org.gradoop.model.impl.id.GradoopId;

public class GraphSimpleEdge<E extends EPGMEdge> implements
  FlatMapFunction<E, Tuple2<GradoopId, SimpleEdge>> {

  @Override
  public void flatMap(E edge,
    Collector<Tuple2<GradoopId, SimpleEdge>> collector) throws
    Exception {

    GradoopId vertexId = edge.getId();
    GradoopId sourceId = edge.getSourceId();
    GradoopId targetId = edge.getTargetId();
    String label = edge.getLabel();

    for(GradoopId graphId : edge.getGraphIds()) {
      collector.collect(new Tuple2<>(graphId,
        new SimpleEdge(vertexId, sourceId, targetId, label)));
    }
  }
}
