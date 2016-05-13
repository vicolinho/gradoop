package org.gradoop.model.impl.algorithms.fsm.filterrefine;

import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;

import java.util.Comparator;

/**
 * Created by peet on 12.05.16.
 */
public class DfsCodeLengthComparator implements Comparator<DFSCode> {
  @Override
  public int compare(DFSCode c1, DFSCode c2) {
    return c1.getSteps().size() - c2.getSteps().size();
  }
}
