package org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.comparators;

import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsStep;

import java.io.Serializable;
import java.util.Comparator;

public class DfsCodeSiblingComparator
  implements Comparator<DfsCode>, Serializable {

  private final DfsStepComparator stepComparator;

  public DfsCodeSiblingComparator(boolean directed) {
    stepComparator = new DfsStepComparator(directed);
  }

  @Override
  public int compare(DfsCode c1, DfsCode c2) {

    DfsStep e1 = c1.getLastStep();
    DfsStep e2 = c2.getLastStep();

    return stepComparator.compare(e1, e2);
  }
}
