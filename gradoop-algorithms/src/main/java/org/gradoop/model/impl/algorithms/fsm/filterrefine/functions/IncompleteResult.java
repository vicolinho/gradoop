package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples.SubgraphMessage;

/**
 * Created by peet on 09.05.16.
 */
public class IncompleteResult implements
  FilterFunction<SubgraphMessage> {

  @Override
  public boolean filter(SubgraphMessage triple
  ) throws
    Exception {

    return triple.f1 < 0;
  }
}
