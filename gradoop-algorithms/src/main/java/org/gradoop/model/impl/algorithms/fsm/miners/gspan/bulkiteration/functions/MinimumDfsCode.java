package org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.comparators.DfsCodeSiblingComparator;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.SerializedSubgraph;

import java.util.Iterator;

public class MinimumDfsCode
  implements GroupReduceFunction<SerializedSubgraph, SerializedSubgraph> {

  private final DfsCodeSiblingComparator comparator;

  public MinimumDfsCode(FSMConfig fsmConfig) {
    comparator = new DfsCodeSiblingComparator(fsmConfig.isDirected());
  }

  @Override
  public void reduce(Iterable<SerializedSubgraph> iterable,
    Collector<SerializedSubgraph> collector) throws Exception {
    Iterator<SerializedSubgraph> iterator = iterable.iterator();

    SerializedSubgraph minCompressedDfsCode = iterator.next();
    DfsCode minDfsCode = minCompressedDfsCode.getDfsCode();

    while (iterator.hasNext()) {
      SerializedSubgraph subgraph = iterator.next();
      DfsCode dfsCode = subgraph.getDfsCode();

      if(comparator.compare(dfsCode, minDfsCode) < 0) {
        minDfsCode = dfsCode;
        minCompressedDfsCode = subgraph;
      }
    }

    collector.collect(minCompressedDfsCode);
  }
}
