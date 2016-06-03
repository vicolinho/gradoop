/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.algorithms.fsm.common.pojos;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * pojo representing a gSpan DFS code
 */
public class DfsCode implements Serializable {
  /**
   * list of steps
   */
  private final List<DfsStep> steps;

  /**
   * constructor
   * @param step initial step
   */
  public DfsCode(DfsStep step) {
    this.steps = new ArrayList<>();
    this.steps.add(step);
  }

  /**
   * constructor
   * @param steps initial steps
   */
  public DfsCode(ArrayList<DfsStep> steps) {
    this.steps = steps;
  }

  /**
   * empty constructor
   */
  public DfsCode() {
    this.steps = new ArrayList<>();
  }

  /**
   * determines vertex times of the rightmost DFS path
   * @return vertex times
   */
  public List<Integer> getRightMostPathVertexTimes() {

    Integer lastFromTime = null;
    Integer lastToTime = null;

    List<Integer> rightMostPath = null;

    for (DfsStep step : Lists.reverse(steps)) {

      if (step.isForward() || lastToTime == null && step.isLoop()) {
        int fromTime = step.getFromTime();
        int toTime = step.getToTime();

        if (lastToTime == null) {
          // graph consists of a single loop
          if (toTime == 0) {
            rightMostPath = Lists.newArrayList(toTime);
          } else {
            rightMostPath = Lists.newArrayList(toTime, fromTime);
          }
        } else if (lastFromTime == toTime) {
          rightMostPath.add(fromTime);
        }

        if (fromTime == 0) {
          break;
        }

        lastFromTime = fromTime;
        lastToTime = toTime;
      }
    }
    return rightMostPath;
  }

  public List<DfsStep> getSteps() {
    return steps;
  }

  @Override
  public String toString() {
    return "[" + StringUtils.join(steps, " ") + "]";
  }

  @Override
  public int hashCode() {

    HashCodeBuilder builder = new HashCodeBuilder();

    for (DfsStep step : steps) {
      builder.append(step.hashCode());
    }

    return builder.hashCode();
  }

  @Override
  public boolean equals(Object other) {

    Boolean equals = this == other;

    if (!equals && other != null && other instanceof DfsCode) {

      DfsCode otherCode = (DfsCode) other;

      if (this.getSteps().size() == otherCode.getSteps().size()) {

        Iterator<DfsStep> ownSteps = this.getSteps().iterator();
        Iterator<DfsStep> otherSteps = otherCode.getSteps().iterator();

        equals = true;

        while (otherSteps.hasNext() && equals) {
          equals = ownSteps.next().equals(otherSteps.next());
        }
      }
    }

    return equals;
  }

  /**
   * deep copy methods
   * @param dfsCode input DFS code
   * @return deep copy of input
   */
  public static DfsCode deepCopy(DfsCode dfsCode) {
    return new DfsCode(Lists.newArrayList(dfsCode.getSteps()));
  }

  public int size() {
    return steps.size();
  }

  public DfsStep getLastStep() {
    return steps.get(steps.size() - 1);
  }

  public int getMinVertexLabel() {
    return steps.isEmpty() ? 0 : steps.get(0).getMinVertexLabel();
  }
}
