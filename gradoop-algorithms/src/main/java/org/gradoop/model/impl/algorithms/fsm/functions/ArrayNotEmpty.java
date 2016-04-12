package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;

/**
 * Created by peet on 12.04.16.
 */
public class ArrayNotEmpty<T> implements FilterFunction<T[]> {
  @Override
  public boolean filter(T[] ts) throws
    Exception {
    return ts.length > 0;
  }
}
