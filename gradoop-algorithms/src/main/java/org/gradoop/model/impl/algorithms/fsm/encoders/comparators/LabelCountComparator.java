package org.gradoop.model.impl.algorithms.fsm.encoders.comparators;

import org.gradoop.model.impl.tuples.WithCount;

import java.util.Comparator;


public class LabelCountComparator implements
  Comparator<WithCount<String>> {
  
  @Override
  public int compare(WithCount<String> label1, WithCount<String> label2) {

    int comparison;

    if (label1.getCount() > label2.getCount()) {
      comparison = -1;
    } else {
      comparison = label1.getObject().compareTo(label2.getObject());
    }

    return comparison;
  }
}

