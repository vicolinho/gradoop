package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;

public class GraphIdSourceIdTargetIdLabel<E extends EPGMEdge> implements
  FlatMapFunction<E,
  Tuple4<GradoopId, GradoopId, GradoopId, String>> {
  @Override
  public void flatMap(E edge,
    Collector<Tuple4<GradoopId, GradoopId, GradoopId, String>> collector) throws
    Exception {

    GradoopId sourceId = edge.getSourceId();
    GradoopId targetId = edge.getTargetId();
    String label = edge.getLabel();

    for (GradoopId graphId : edge.getGraphIds()) {
      collector.collect(
        new Tuple4<>(graphId, sourceId, targetId, label));
    }
  }
}
