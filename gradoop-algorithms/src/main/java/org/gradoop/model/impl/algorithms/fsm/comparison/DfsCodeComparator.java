package org.gradoop.model.impl.algorithms.fsm.comparison;

import org.gradoop.model.impl.algorithms.fsm.pojos.DfsCode;

import java.io.Serializable;
import java.util.Comparator;

public class DfsCodeComparator implements Comparator<DfsCode>, Serializable {
  private final boolean directed;

  public DfsCodeComparator(boolean directed) {
    this.directed = directed;
  }

  @Override
  public int compare(DfsCode o1, DfsCode o2) {
    return 0;
  }
}
