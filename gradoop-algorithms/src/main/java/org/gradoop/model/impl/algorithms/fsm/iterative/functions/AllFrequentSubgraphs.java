package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.SerializedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.ObjectWithCount;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;

import java.util.Collection;

/**
 * Created by peet on 26.05.16.
 */
public class AllFrequentSubgraphs implements
  MapFunction<IterationItem, Collection<ObjectWithCount<CompressedSubgraph>>> {
  @Override
  public Collection<ObjectWithCount<CompressedSubgraph>> map(IterationItem iterationItem) throws
    Exception {

//    System.out.println(iterationItem.getFrequentSubgraphs());

    return iterationItem.getFrequentSubgraphs();
  }
}
