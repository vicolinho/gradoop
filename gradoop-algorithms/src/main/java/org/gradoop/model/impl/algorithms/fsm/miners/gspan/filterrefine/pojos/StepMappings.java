package org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.pojos;

import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsStep;

import java.util.Collection;

public class StepMappings
  implements Comparable<StepMappings> {

  private final DfsStep step;
  private final Collection<StepPatternMapping> mappings;
  private final int frequency;
  private final int edgeTime;

  public StepMappings(DfsStep stepPattern,
    Collection<StepPatternMapping> mappings, int edgeTime) {
    this.step = stepPattern;
    this.mappings = mappings;
    this.frequency = mappings.size();
    this.edgeTime = edgeTime;
  }

  @Override
  public int compareTo(StepMappings o) {
    return this.frequency - o.frequency;
  }

  public DfsStep getStep() {
    return step;
  }

  public Collection<StepPatternMapping> getMappings() {
    return mappings;
  }

  public int getEdgeTime() {
    return edgeTime;
  }
}
