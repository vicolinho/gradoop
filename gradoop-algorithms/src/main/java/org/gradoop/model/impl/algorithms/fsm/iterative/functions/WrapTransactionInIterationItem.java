package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.GSpanTransaction;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;

public class WrapTransactionInIterationItem
  implements MapFunction<GSpanTransaction, IterationItem> {

  @Override
  public IterationItem map(GSpanTransaction gSpanTransaction) throws Exception {
    return new IterationItem(gSpanTransaction);
  }
}
