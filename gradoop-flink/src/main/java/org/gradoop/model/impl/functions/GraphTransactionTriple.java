package org.gradoop.model.impl.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphTransaction;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Set;

public class GraphTransactionTriple
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements MapFunction<GraphTransaction<G, V, E>, Tuple3<G, Set<V>, Set<E>>>,
  JoinFunction<Tuple3<GradoopId, Set<V>, Set<E>>, G, GraphTransaction<G, V, E>>
{
  @Override
  public Tuple3<G, Set<V>, Set<E>> map(
    GraphTransaction<G, V, E> transaction) throws Exception {

    return new Tuple3<>(transaction.f0, transaction.f1, transaction.f2);
  }

  @Override
  public GraphTransaction<G, V, E> join(
    Tuple3<GradoopId, Set<V>, Set<E>> triple, G graphHead) throws Exception {
    return new GraphTransaction<>(graphHead, triple.f1, triple.f2);
  }
}
