package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan
  .DfsCodeSiblingComparator;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;

import java.util.Iterator;

public class MinimumDfsCode
  implements GroupReduceFunction<CompressedDFSCode, CompressedDFSCode> {

  private final DfsCodeSiblingComparator comparator;

  public MinimumDfsCode(FSMConfig fsmConfig) {
    comparator = new DfsCodeSiblingComparator(fsmConfig.isDirected());
  }

  @Override
  public void reduce(Iterable<CompressedDFSCode> iterable,
    Collector<CompressedDFSCode> collector) throws Exception {
    Iterator<CompressedDFSCode> iterator = iterable.iterator();

    DFSCode minDfsCode = iterator.next().getDfsCode();

    while (iterator.hasNext()) {
      DFSCode dfsCode = iterator.next().getDfsCode();

      if(comparator.compare(dfsCode, minDfsCode) < 0) {
        minDfsCode = dfsCode;
      }
    }

    collector.collect(new CompressedDFSCode(minDfsCode));
  }
}
