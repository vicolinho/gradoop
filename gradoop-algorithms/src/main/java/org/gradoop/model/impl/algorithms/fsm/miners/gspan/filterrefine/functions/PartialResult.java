package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.tuples
  .RefinementMessage;

/**
 * Created by peet on 09.05.16.
 */
public class PartialResult implements FilterFunction<RefinementMessage> {


  @Override
  public boolean filter(RefinementMessage refinementMessage) throws Exception {
    return refinementMessage.getMessageType() == RefinementMessage.PARTIAL_RESULT;
  }
}
