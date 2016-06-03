package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;

import java.util.Collection;

/**
 * Created by peet on 30.05.16.
 */
public class WrapCollectorInIterationItem
  implements MapFunction<Collection<WithCount<CompressedSubgraph>>, IterationItem> {

  @Override
  public IterationItem map(
    Collection<WithCount<CompressedSubgraph>> subgraph) throws Exception {
    return new IterationItem(subgraph);
  }
}
