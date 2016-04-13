package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.impl.algorithms.fsm.tuples.StringLabeledEdge;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;


public class FullEdge<E extends EPGMEdge> implements
  MapFunction<Tuple2<GradoopId, StringLabeledEdge>, E> {
  private final EPGMEdgeFactory<E> edgeFactory;

  public FullEdge(EPGMEdgeFactory<E> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }


  @Override
  public E map(
    Tuple2<GradoopId, StringLabeledEdge> graphIdEdge) throws Exception {
    StringLabeledEdge edge = graphIdEdge.f1;
    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphIdEdge.f0);

    return edgeFactory.initEdge(edge.getId(), edge.getLabel(),
      edge.getSourceId(), edge.getTargetId(),  graphIds);
  }
}
