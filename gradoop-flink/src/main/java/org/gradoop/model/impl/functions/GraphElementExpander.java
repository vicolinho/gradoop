package org.gradoop.model.impl.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;

public class GraphElementExpander<EL extends EPGMGraphElement> implements
  FlatMapFunction<EL, Tuple2<GradoopId, EL>> {
  @Override
  public void flatMap(EL el, Collector<Tuple2<GradoopId, EL>> collector) throws
    Exception {

    for(GradoopId graphId : el.getGraphIds()) {
      collector.collect(new Tuple2<>(graphId, el));
    }
  }
}
