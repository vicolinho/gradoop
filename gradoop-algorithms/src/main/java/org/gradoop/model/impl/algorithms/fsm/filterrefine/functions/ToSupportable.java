package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.Supportable;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples.SubgraphMessage;

/**
 * Created by peet on 02.06.16.
 */
public class ToSupportable
  implements MapFunction<SubgraphMessage, Supportable<CompressedDfsCode>> {

  @Override
  public Supportable<CompressedDfsCode> map(SubgraphMessage message) throws Exception {
    return new Supportable<>(message.getSubgraph(), message.getSupport());
  }
}
