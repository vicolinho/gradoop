package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.Transaction;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by peet on 09.05.16.
 */
public class Refinement implements
  FlatJoinFunction<Tuple2<Integer, Collection<CompressedDFSCode>>,
    Tuple2<Integer, Map<Integer, Transaction>>, CompressedDFSCode> {

  @Override
  public void join(
    Tuple2<Integer, Collection<CompressedDFSCode>> codePair,
    Tuple2<Integer, Map<Integer, Transaction>> transactionPair,
    Collector<CompressedDFSCode> collector) throws Exception {

    Map<DFSCode, Set<DFSCode>> codeTree = Maps.newHashMap();

    for(CompressedDFSCode maxCode : codePair.f1) {
      DFSCode parent = new DFSCode();

      for(DFSStep step : maxCode.getDfsCode().getSteps()) {

        DFSCode child = DFSCode.deepCopy(parent);
        child.getSteps().add(step);

        Set<DFSCode> siblings = codeTree.get(parent);

        if(siblings == null) {
          codeTree.put(parent, Sets.newHashSet(child));
        } else {
          siblings.add(child);
        }

        parent = child;
      }
    }

    System.out.println(codeTree);
  }
}
