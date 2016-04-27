package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 27.04.16.
 */
public class VertexIdLabelOnly<V extends EPGMVertex> implements
  MapFunction<V, Tuple2<GradoopId, String>> {
  @Override
  public Tuple2<GradoopId, String> map(V v) throws Exception {
    return new Tuple2<>(v.getId(), v.getLabel());
  }
}
