package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.ObjectWithCount;

import java.util.Collection;

public class Merge implements ReduceFunction<Collection<ObjectWithCount<CompressedSubgraph>>> {

  @Override
  public Collection<ObjectWithCount<CompressedSubgraph>> reduce(
    Collection<ObjectWithCount<CompressedSubgraph>> firstCollection,
    Collection<ObjectWithCount<CompressedSubgraph>> secondCollection) throws Exception {


    Collection<ObjectWithCount<CompressedSubgraph>> mergedCollection;

    if(firstCollection.size() >= firstCollection.size()) {
      firstCollection.addAll(secondCollection);
      mergedCollection = firstCollection;
    } else {
      secondCollection.addAll(firstCollection);
      mergedCollection = secondCollection;
    }

//    System.out.println(firstCollection +
//      "\n+" + secondCollection +
//      "\n=" + mergedCollection);

    return mergedCollection;
  }
}
