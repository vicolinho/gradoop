package org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;

public class SubgraphMessage extends Tuple4<CompressedDfsCode, Integer, Integer, Boolean> {

  public SubgraphMessage() {

  }

  public SubgraphMessage(CompressedDfsCode subgraph, int support, int workerId, boolean locallyFrequent) {
    super(subgraph, support, workerId, locallyFrequent);
  }

  public int getSupport() {
    return this.f1;
  }

  public CompressedDfsCode getSubgraph() {
    return this.f0;
  }

  public boolean needsRefinement() {
    return !this.f3;
  }
}
