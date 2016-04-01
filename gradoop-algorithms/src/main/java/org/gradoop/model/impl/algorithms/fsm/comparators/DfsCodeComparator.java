package org.gradoop.model.impl.algorithms.fsm.comparators;

import org.gradoop.model.impl.algorithms.fsm.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DfsStep;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;

public class DfsCodeComparator implements Comparator<DfsCode>, Serializable {

  private final DfsStepComparator dfsStepComparator;

  public DfsCodeComparator(boolean directed) {
    dfsStepComparator = new DfsStepComparator(directed);
  }

  @Override
  public int compare(DfsCode c1, DfsCode c2) {
    int comparison = 0;

    Iterator<DfsStep> i1 = c1.getSteps().iterator();
    Iterator<DfsStep> i2 = c2.getSteps().iterator();

    // compare steps until there is a difference
    while (comparison == 0 && i1.hasNext() && i2.hasNext()) {
      DfsStep s1 = i1.next();
      DfsStep s2 = i2.next();

      comparison = dfsStepComparator.compare(s1, s2);
    }

    // common parent
    if(comparison == 0) {
      // c1 is child of c2
      if (i1.hasNext()) {
        comparison = -1;
        // c2 is child of c1
      } else if (i2.hasNext()) {
        comparison = 1;
      }
    }

    return comparison;
  }
}
