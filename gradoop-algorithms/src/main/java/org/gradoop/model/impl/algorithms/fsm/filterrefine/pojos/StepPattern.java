package org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos;

import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSStep;

public class StepPattern extends StepTriple {


  public StepPattern(int sourceId, int edgeId, int targetId) {
    super(sourceId, edgeId, targetId);
  }

  public StepPattern(DFSStep step) {
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
