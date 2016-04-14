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

package org.gradoop.model.impl.algorithms.fsm.pojos;

import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * pojo representing a gSpan DFS code
 */
public class DFSCode implements Serializable {
  /**
   * list of steps
   */
  private final ArrayList<DFSStep> steps;

  /**
   * constructor
   * @param step initial step
   */
  public DFSCode(DFSStep step) {
    this.steps = new ArrayList<>();
    this.steps.add(step);
  }

  /**
   * constructor
   * @param steps initial steps
   */
  public DFSCode(ArrayList<DFSStep> steps) {
    this.steps = steps;
  }

  /**
   * empty constructor
   */
  public DFSCode() {
    this.steps = new ArrayList<>();
  }

  /**
   * returns the edge pattern of the first step
   * @return edge pattern
   */
  public EdgePattern<Integer> getMinEdgePattern() {
    DFSStep firstStep = steps.get(0);

    return new EdgePattern<>(
      firstStep.getFromLabel(),
      firstStep.isOutgoing(),
      firstStep.getEdgeLabel(),
      firstStep.getToLabel()
    );
  }

  /**
   * determines vertex times of the rightmost DFS path
   * @return vertex times
   */
  public List<Integer> getRightMostPathVertexTimes() {

    Integer lastFromTime = null;
    Integer lastToTime = null;

    List<Integer> rightMostPath = null;

    for (DFSStep step : Lists.reverse(steps)) {

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

  public ArrayList<DFSStep> getSteps() {
    return steps;
  }

  @Override
  public String toString() {
//    Collection<String> vertexLabels = Lists
//      .newArrayList();
//    Collection<String> edgeLabels = Lists
//      .newArrayListWithExpectedSize(steps.size());
//
//    for(DFSStep step : steps) {
//
//      int fromTime = step.getFromTime();
//      int toTime = step.getToTime();
//
//      if(vertexLabels.isEmpty()) {
//        vertexLabels.add("(" + fromTime + ":" + step.getFromLabel() + ")");
//      }
//      if(toTime > fromTime) {
//        vertexLabels.add("(" + toTime + ":" + step.getToLabel() + ")");
//      }
//
//      edgeLabels.add("(" + fromTime + ")" +
//        (step.isOutgoing() ?
//        "-" + step.getEdgeLabel() + "->" : "<-" + step.getEdgeLabel()) +
//        "(" + toTime + ")");
//    }
//
//    return "\n" + StringUtils.join(vertexLabels,",") + "\n\t" +
//      StringUtils.join(edgeLabels, ",");
    return steps.toString();
  }

  @Override
  public int hashCode() {

    HashCodeBuilder builder = new HashCodeBuilder();

    for (DFSStep step : steps) {
      builder.append(step.hashCode());
    }

    return builder.hashCode();
  }

  @Override
  public boolean equals(Object other) {

    Boolean equals = this == other;

    if (!equals && other != null && other instanceof DFSCode) {

      DFSCode otherCode = (DFSCode) other;

      if (this.getSteps().size() == otherCode.getSteps().size()) {

        Iterator<DFSStep> ownSteps = this.getSteps().iterator();
        Iterator<DFSStep> otherSteps = otherCode.getSteps().iterator();

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
  public static DFSCode deepCopy(DFSCode dfsCode) {
    return new DFSCode(Lists.newArrayList(dfsCode.getSteps()));
  }
}
