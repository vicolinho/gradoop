package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan
  .DfsCodeSiblingComparator;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;

import java.util.Iterator;

public class MinimumDfsCode
  implements GroupReduceFunction<CompressedDfsCode, CompressedDfsCode> {

  private final DfsCodeSiblingComparator comparator;

  public MinimumDfsCode(FSMConfig fsmConfig) {
    comparator = new DfsCodeSiblingComparator(fsmConfig.isDirected());
  }

  @Override
  public void reduce(Iterable<CompressedDfsCode> iterable,
    Collector<CompressedDfsCode> collector) throws Exception {
    Iterator<CompressedDfsCode> iterator = iterable.iterator();

    CompressedDfsCode minCompressedDfsCode = iterator.next();
    DfsCode minDfsCode = minCompressedDfsCode.getDfsCode();

    while (iterator.hasNext()) {
      CompressedDfsCode subgraph = iterator.next();
      DfsCode dfsCode = subgraph.getDfsCode();

      if(comparator.compare(dfsCode, minDfsCode) < 0) {
        minDfsCode = dfsCode;
        minCompressedDfsCode = subgraph;
      }
    }

    collector.collect(minCompressedDfsCode);
  }
}
