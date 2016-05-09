package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;

/**
 * Created by peet on 09.05.16.
 */
public class NeedsRefinement implements
  FilterFunction<Tuple3<CompressedDFSCode, Integer, Boolean>> {

  @Override
  public boolean filter(Tuple3<CompressedDFSCode, Integer, Boolean> triple
  ) throws
    Exception {

    return !triple.f2;
  }
}
