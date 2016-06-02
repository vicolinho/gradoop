package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.Supportable;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;

import java.util.Collection;

/**
 * Created by peet on 30.05.16.
 */
public class WrapCollectorInIterationItem
  implements MapFunction<Collection<Supportable<CompressedDfsCode>>, IterationItem> {

  @Override
  public IterationItem map(
    Collection<Supportable<CompressedDfsCode>> subgraph) throws Exception {
    return new IterationItem(subgraph);
  }
}
