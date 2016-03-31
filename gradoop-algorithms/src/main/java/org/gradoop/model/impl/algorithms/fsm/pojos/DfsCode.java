package org.gradoop.model.impl.algorithms.fsm.pojos;

import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

public class DfsCode implements Serializable {
  private final ArrayList<DfsStep> steps;

  public DfsCode(DfsStep step) {
    this.steps = Lists.newArrayList(step);
  }

  public DfsCode() {
    this.steps = new ArrayList<>();
  }

  public DfsCode(ArrayList<DfsStep> steps) {
    this.steps = steps;
  }

  @Override
  public String toString() {
    return steps.toString();
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    for(DfsStep step : steps) {
      builder.append(step.hashCode());
    }

    return builder.hashCode();
  }

  public ArrayList<DfsStep> getSteps() {
    return steps;
  }

  @Override
  public boolean equals(Object other) {

    Boolean equals = this == other;

    if(!equals && other != null && other instanceof DfsCode ) {

      DfsCode otherCode = (DfsCode) other;

      if(this.getSteps().size() == otherCode.getSteps().size()) {

        Iterator<DfsStep> ownSteps = this.getSteps().iterator();
        Iterator<DfsStep> otherSteps = otherCode.getSteps().iterator();

        equals = true;

        while(otherSteps.hasNext() && equals) {
          equals = ownSteps.next().equals(otherSteps.next());
        }
      }
    }

    return equals;
  }


  public int getEdgeCount() {
    return steps.size();
  }

  public int getVertexCount() {

    int maxTime = 0;

    for (DfsStep step : getSteps()) {
      if(step.getFromTime() > maxTime) {
        maxTime = step.getFromTime();
      }
      if(step.getToTime() > maxTime) {
        maxTime = step.getToTime();
      }
    }

    return maxTime;
  }

  public static DfsCode deepCopy(DfsCode dfsCode) {
    return new DfsCode(Lists.newArrayList(dfsCode.getSteps()));
  }
}
