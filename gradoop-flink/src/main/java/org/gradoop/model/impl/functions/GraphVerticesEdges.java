package org.gradoop.model.impl.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Set;

public class GraphVerticesEdges<V extends EPGMVertex, E extends EPGMEdge>
  implements JoinFunction<Tuple2<GradoopId, Set<V>>, Tuple2<GradoopId, Set<E>>,
  Tuple3<GradoopId, Set<V>, Set<E>>> {

  @Override
  public Tuple3<GradoopId, Set<V>, Set<E>> join(
    Tuple2<GradoopId, Set<V>> vertices, Tuple2<GradoopId, Set<E>> edges) throws
    Exception {

    return new Tuple3<>(vertices.f0, vertices.f1, edges.f1);
  }
}
