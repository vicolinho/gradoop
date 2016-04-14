package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;


public class FullVertex<V extends EPGMVertex> implements
  MapFunction<Tuple3<GradoopId, GradoopId, String>, V> {
  private final EPGMVertexFactory<V> vertexFactory;

  public FullVertex(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }


  @Override
  public V map(
    Tuple3<GradoopId, GradoopId, String> gidVidLabel) throws Exception {
    return vertexFactory.initVertex(
      gidVidLabel.f1, gidVidLabel.f2,
      GradoopIdSet.fromExisting(gidVidLabel.f0));
  }
}
