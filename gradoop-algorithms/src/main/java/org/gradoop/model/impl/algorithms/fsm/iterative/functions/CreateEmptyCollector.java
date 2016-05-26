package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;


public class CreateEmptyCollector implements MapFunction<Boolean, IterationItem> {
  @Override
  public IterationItem map(Boolean aBoolean) throws Exception {
    return new IterationItem(Lists.<CompressedDFSCode>newArrayList(), false);
  }
}
