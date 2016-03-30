package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.Collection;

public class GraphElements<EL> implements GroupReduceFunction
  <Tuple2<GradoopId, EL>, Tuple2<GradoopId, Collection<EL>>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, EL>> iterable,
    Collector<Tuple2<GradoopId, Collection<EL>>> collector) throws Exception {

    Boolean first = true;
    GradoopId graphId = null;
    Collection<EL> elements = new ArrayList<>();

    for (Tuple2<GradoopId, EL> pair : iterable) {
      if (first) {
        first = false;
        graphId = pair.f0;
      }

      elements.add(pair.f1);
    }

    collector.collect(new Tuple2<>(graphId, elements));
  }
}
