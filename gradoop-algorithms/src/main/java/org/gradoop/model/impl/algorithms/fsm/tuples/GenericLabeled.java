package org.gradoop.model.impl.algorithms.fsm.tuples;

public interface GenericLabeled<L extends Comparable<L>> {

  public L getLabel();
  public void setLabel(L label);
}
