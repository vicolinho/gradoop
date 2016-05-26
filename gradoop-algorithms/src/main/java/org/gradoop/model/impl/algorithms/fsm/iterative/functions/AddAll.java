package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;

import java.util.Collection;

public class AddAll implements ReduceFunction<Collection<CompressedDFSCode>> {

  @Override
  public Collection<CompressedDFSCode> reduce(
    Collection<CompressedDFSCode> firstCollection,
    Collection<CompressedDFSCode> secondCollection) throws Exception {


    Collection<CompressedDFSCode> mergedCollection;

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
