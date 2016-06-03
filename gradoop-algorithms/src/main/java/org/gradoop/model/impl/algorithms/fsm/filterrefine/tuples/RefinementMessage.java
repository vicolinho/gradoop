package org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;

public class RefinementMessage
  extends Tuple3<CompressedSubgraph, Integer, Short> {
  public static final short GLOBALLY_FREQUENT = 0;
  public static final short PARTIAL_RESULT = 1;
  public static final short REFINEMENT_CALL = 2;

  public RefinementMessage() {

  }


  public RefinementMessage(CompressedSubgraph subgraph, int supportOrWorkerId,
    short messageType) {
    super(subgraph, supportOrWorkerId, messageType);

  }

  public short getMessageType() {
    return this.f2;
  }

  public int getSupport() {
    return this.f1;
  }

  public CompressedSubgraph getSubgraph() {
    return this.f0;
  }

  public int getWorkerId() {
    return this.f1;
  }
}
