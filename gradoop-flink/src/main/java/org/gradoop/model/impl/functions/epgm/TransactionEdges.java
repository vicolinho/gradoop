package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;

import java.util.Set;

public class TransactionEdges
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements FlatMapFunction<Tuple3<G, Set<V>, Set<E>>, E> {

  @Override
  public void flatMap(Tuple3<G, Set<V>, Set<E>> triple,
    Collector<E> collector) throws Exception {

    for(E edge : triple.f2) {
      collector.collect(edge);
    }
  }
}
