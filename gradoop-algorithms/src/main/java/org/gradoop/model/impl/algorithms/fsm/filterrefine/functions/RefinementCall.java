package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples.FilterResult;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples
  .RefinementMessage;


public class RefinementCall implements
  FilterFunction<RefinementMessage> {


  @Override
  public boolean filter(RefinementMessage refinementMessage) throws Exception {
    return refinementMessage.getMessageType() == RefinementMessage.REFINEMENT_CALL;
  }
}
