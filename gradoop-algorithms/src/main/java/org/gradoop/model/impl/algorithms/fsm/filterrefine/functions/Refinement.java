package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.Transaction;

import java.util.Collection;
import java.util.Map;

/**
 * Created by peet on 09.05.16.
 */
public class Refinement implements
  FlatJoinFunction<Tuple2<Integer, Collection<CompressedDFSCode>>,
    Tuple2<Integer, Map<Integer, Transaction>>, CompressedDFSCode> {

  @Override
  public void join(
    Tuple2<Integer, Collection<CompressedDFSCode>> integerCollectionTuple2,
    Tuple2<Integer, Map<Integer, Transaction>> integerMapTuple2,
    Collector<CompressedDFSCode> collector) throws Exception {

  }
}
