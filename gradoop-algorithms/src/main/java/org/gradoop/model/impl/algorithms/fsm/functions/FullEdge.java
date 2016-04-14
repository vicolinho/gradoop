package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;


public class FullEdge<E extends EPGMEdge> implements
  MapFunction<Tuple4<GradoopId, GradoopId, GradoopId, String>, E> {
  private final EPGMEdgeFactory<E> edgeFactory;

  public FullEdge(EPGMEdgeFactory<E> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }


  @Override
  public E map(
    Tuple4<GradoopId, GradoopId, GradoopId, String> edge) throws Exception {

    return edgeFactory.createEdge(
      edge.f3, edge.f1, edge.f2, GradoopIdSet.fromExisting(edge.f0));
  }
}
