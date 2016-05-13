package org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos;

import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSStep;

import java.util.Collection;

public class StepMappings
  implements Comparable<StepMappings> {

  private final DFSStep step;
  private final Collection<StepPatternMapping> mappings;
  private final int frequency;
  private final int edgeTime;

  public StepMappings(DFSStep stepPattern,
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

  public DFSStep getStep() {
    return step;
  }

  public Collection<StepPatternMapping> getMappings() {
    return mappings;
  }

  public int getEdgeTime() {
    return edgeTime;
  }
}
