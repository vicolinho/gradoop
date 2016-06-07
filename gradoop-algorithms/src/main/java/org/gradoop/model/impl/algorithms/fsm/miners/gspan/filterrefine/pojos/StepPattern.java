package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.pojos;

import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsStep;

public class StepPattern extends StepTriple {


  public StepPattern(int sourceId, int edgeId, int targetId) {
    super(sourceId, edgeId, targetId);
  }

  public StepPattern(DfsStep step) {
    super(
      step.isOutgoing() ? step.getFromLabel() : step.getToLabel(),
      step.getEdgeLabel(),
      step.isOutgoing() ? step.getToLabel() : step.getFromLabel()
    );
  }

  public int getSourceLabel() {
    return sourceValue;
  }

  public int getEdgeLabel() {
    return edgeValue;
  }

  public int getTargetLabel() {
    return targetValue;
  }

}
