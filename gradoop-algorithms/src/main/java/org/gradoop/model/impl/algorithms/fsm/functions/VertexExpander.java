package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Collection;

public class VertexExpander<G, V, E>
  implements FlatMapFunction<Tuple3<G, Collection<V>, Collection<E>>, V> {
  @Override
  public void flatMap(Tuple3<G, Collection<V>, Collection<E>> triple,
    Collector<V> collector) throws Exception {

    for (V vertex : triple.f1) {
      collector.collect(vertex);
    }
  }
}


