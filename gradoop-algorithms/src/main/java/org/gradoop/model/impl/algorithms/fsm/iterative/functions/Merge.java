package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;

import java.util.Collection;

public class Merge implements ReduceFunction<Collection<CompressedDfsCode>> {

  @Override
  public Collection<CompressedDfsCode> reduce(
    Collection<CompressedDfsCode> firstCollection,
    Collection<CompressedDfsCode> secondCollection) throws Exception {


    Collection<CompressedDfsCode> mergedCollection;

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
