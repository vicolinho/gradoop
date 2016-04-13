package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.algorithms.fsm.tuples.LabeledVertex;
import org.gradoop.model.impl.algorithms.fsm.tuples.StringLabeledVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;


public class FullVertex<V extends EPGMVertex> implements
  MapFunction<Tuple2<GradoopId, StringLabeledVertex>, V> {
  private final EPGMVertexFactory<V> vertexFactory;

  public FullVertex(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }


  @Override
  public V map(
    Tuple2<GradoopId, StringLabeledVertex> graphIdVertex) throws Exception {
    StringLabeledVertex vertex = graphIdVertex.f1;
    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphIdVertex.f0);

    return vertexFactory
      .initVertex(vertex.getId(), vertex.getLabel(), graphIds);
  }
}
