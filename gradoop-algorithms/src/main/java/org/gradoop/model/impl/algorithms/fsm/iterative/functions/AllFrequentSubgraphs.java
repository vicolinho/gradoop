package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;

import java.util.Collection;

/**
 * Created by peet on 26.05.16.
 */
public class AllFrequentSubgraphs implements
  MapFunction<IterationItem, Collection<CompressedDFSCode>> {
  @Override
  public Collection<CompressedDFSCode> map(IterationItem iterationItem) throws
    Exception {

//    System.out.println(iterationItem.getFrequentSubgraphs());

    return iterationItem.getFrequentSubgraphs();
  }
}
