package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.pojos;


public class StepPatternMapping extends StepTriple {

  public StepPatternMapping(int sourceId, int edgeId, int targetId) {
    super(sourceId, edgeId, targetId);
  }

  public int getSourceId() {
    return sourceValue;
  }

  public int getEdgeId() {
    return edgeValue;
  }

  public int getTargetId() {
    return targetValue;
  }
}
