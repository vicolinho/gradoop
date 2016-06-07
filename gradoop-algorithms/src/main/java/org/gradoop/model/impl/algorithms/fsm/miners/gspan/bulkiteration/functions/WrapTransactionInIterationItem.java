package org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.GSpanTransaction;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.tuples.IterationItem;

public class WrapTransactionInIterationItem
  implements MapFunction<GSpanTransaction, IterationItem> {

  @Override
  public IterationItem map(GSpanTransaction gSpanTransaction) throws Exception {
    return new IterationItem(gSpanTransaction);
  }
}
