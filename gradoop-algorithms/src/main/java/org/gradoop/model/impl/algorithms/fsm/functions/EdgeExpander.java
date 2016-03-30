package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Collection;

public class EdgeExpander<G, V, E>
  implements FlatMapFunction<Tuple3<G, Collection<V>, Collection<E>>, E> {
  @Override
  public void flatMap(Tuple3<G, Collection<V>, Collection<E>> triple,
    Collector<E> collector) throws Exception {

    for (E edge : triple.f2) {
      collector.collect(edge);
    }
  }
}