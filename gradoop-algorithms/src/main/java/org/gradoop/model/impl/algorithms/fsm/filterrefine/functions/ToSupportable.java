package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.ObjectWithCount;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples.SubgraphMessage;

/**
 * Created by peet on 02.06.16.
 */
public class ToSupportable
  implements MapFunction<SubgraphMessage, ObjectWithCount<CompressedSubgraph>> {

  @Override
  public ObjectWithCount<CompressedSubgraph> map(SubgraphMessage message) throws Exception {
    return new ObjectWithCount<>(message.getSubgraph(), message.getSupport());
  }
}
