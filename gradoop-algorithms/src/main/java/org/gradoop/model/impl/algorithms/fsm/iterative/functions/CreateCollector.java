package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.Transaction;

public class CreateCollector implements MapFunction<Boolean, Transaction> {
  @Override
  public Transaction map(Boolean aBoolean) throws Exception {
    return Transaction.createCollector();
  }
}
