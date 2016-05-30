package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;

import java.util.Collection;

/**
 * Created by peet on 30.05.16.
 */
public class WrapCollectorInIterationItem
  implements MapFunction<Collection<CompressedDFSCode>, IterationItem> {

  @Override
  public IterationItem map(
    Collection<CompressedDFSCode> compressedDFSCodes) throws Exception {
    return new IterationItem(compressedDFSCodes);
  }
}
