package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.Supportable;

import java.util.Collection;

public class Merge implements ReduceFunction<Collection<Supportable<CompressedDfsCode>>> {

  @Override
  public Collection<Supportable<CompressedDfsCode>> reduce(
    Collection<Supportable<CompressedDfsCode>> firstCollection,
    Collection<Supportable<CompressedDfsCode>> secondCollection) throws Exception {


    Collection<Supportable<CompressedDfsCode>> mergedCollection;

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
