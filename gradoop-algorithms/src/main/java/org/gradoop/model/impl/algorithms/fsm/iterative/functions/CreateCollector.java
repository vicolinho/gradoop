package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.TransactionWrapper;


public class CreateCollector implements MapFunction<Boolean, TransactionWrapper> {
  @Override
  public TransactionWrapper map(Boolean aBoolean) throws Exception {
    return TransactionWrapper.createCollector();
  }
}
