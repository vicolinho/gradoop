package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;

import java.util.Set;

public class TransactionGraphHead
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements MapFunction<Tuple3<G, Set<V>, Set<E>>, G> {

  @Override
  public G map(Tuple3<G, Set<V>, Set<E>> triple) throws Exception {
    return triple.f0;
  }
}
