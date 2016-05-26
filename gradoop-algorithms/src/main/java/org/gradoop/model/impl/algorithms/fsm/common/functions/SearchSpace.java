package org.gradoop.model.impl.algorithms.fsm.common.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;

import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.IntegerLabeledEdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.GSpanTransaction;


import org.gradoop.model.impl.id.GradoopId;

public class SearchSpace implements GroupReduceFunction
  <Tuple3<GradoopId, IntegerLabeledEdgeTriple, CompressedDFSCode>, GSpanTransaction> {

  private final FSMConfig fsmConfig;

  public SearchSpace(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void reduce(
    Iterable<Tuple3<GradoopId, IntegerLabeledEdgeTriple, CompressedDFSCode>> iterable,
    Collector<GSpanTransaction> collector) throws Exception {

    collector.collect(GSpan.createTransaction(iterable, fsmConfig));
  }
}
