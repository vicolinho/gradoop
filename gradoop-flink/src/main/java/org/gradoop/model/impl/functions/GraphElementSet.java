package org.gradoop.model.impl.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Set;

public class GraphElementSet<EL extends EPGMGraphElement> implements
  GroupReduceFunction<Tuple2<GradoopId, EL>, Tuple2<GradoopId, Set<EL>>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, EL>> iterable,
    Collector<Tuple2<GradoopId, Set<EL>>> collector) throws Exception {

    boolean first = true;
    GradoopId graphId = null;

    Set<EL> elements = Sets.newHashSet();

    for(Tuple2<GradoopId, EL> elementPair : iterable) {

      if(first) {
        graphId = elementPair.f0;
        first = false;
      }

      elements.add(elementPair.f1);
    }

    collector.collect(new Tuple2<>(graphId, elements));
  }
}
