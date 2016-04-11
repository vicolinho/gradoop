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

package org.gradoop.model.impl.operators.tostring.comparators;

import org.gradoop.model.impl.operators.tostring.pojos.DFSStep;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Comparator of DFS code steps based on gSpan lexicographical order.
 */
public class DfsStepComparator implements Comparator<DFSStep>, Serializable {

  /**
   * true for comparing DFS steps of directed graphs,
   * false for undirected graphs
   */
  private final boolean directed;

  /**
   * constructor
   * @param directed true for comparing DFS steps of directed graphs
   */
  public DfsStepComparator(boolean directed) {
    this.directed = directed;
  }

  @Override
  public int compare(DFSStep s1, DFSStep s2) {
    int comparison;

    // same direction
    if (s1.isForward() && s2.isForward()) {

      // both forward
      if (s1.isForward()) {

        // starts from same position
        if (s1.getFromTime() == s2.getFromTime()) {

          // inherit edge comparison by labels (lexicographically order)
          comparison = compareLabels(s1, s2);

          // starts from a later visited vertex
        } else if (s1.getFromTime() > s2.getFromTime()) {
          comparison = -1;

          // starts from an earlier visited vertex
        } else {
          comparison = 1;
        }

        // both backward
      } else {

        // refers same position
        if (s1.getToTime() == s2.getToTime()) {

          // inherit edge comparison by labels (lexicographically order)
          comparison = compareLabels(s1, s2);

          // refers an earlier visited vertex
        } else if (s1.getToTime() < s2.getToTime()) {
          comparison = -1;

          // refers a later visited vertex
        } else {
          comparison = 1;
        }
      }

      // inverse direction
    } else {
      if (s1.isBackward()) {
        comparison = -1;
      } else {
        comparison = 1;
      }
    }

    return comparison;
  }

  /**
   * extracted method to compare DFS steps based on start, edge and edge labels
   * as well as the traversal direction
   * @param s1 first DFS step
   * @param s2 second DFS step
   * @return comparison result
   */
  private int compareLabels(DFSStep s1, DFSStep s2) {
    int comparison;

    if (s1.getFromLabel().compareTo(s2.getFromLabel()) < 0) {
      comparison = -1;
    } else if (s1.getFromLabel().compareTo(s2.getFromLabel()) > 0) {
      comparison = 1;
    } else {
      if (directed) {
        comparison = compareDirectedLabels(s1, s2);
      } else {
        comparison = compareUndirectedLabels(s1, s2);
      }
    }
    return comparison;
  }

  /**
   * label comparison for directed mode
   * @param s1 first DFS step
   * @param s2 second DFS step
   * @return  comparison result
   */
  private int compareDirectedLabels(DFSStep s1, DFSStep s2) {
    int comparison;

    if (s1.isOutgoing() && !s2.isOutgoing()) {
      comparison = -1;
    } else if (!s1.isOutgoing() && s2.isOutgoing()) {
      comparison = 1;
    } else {
      comparison = compareUndirectedLabels(s1, s2);
    }
    return comparison;
  }

  /**
   * label comparison for undirected mode
   * @param s1 first DFS step
   * @param s2 second DFS step
   * @return  comparison result
   */
  private int compareUndirectedLabels(DFSStep s1, DFSStep s2) {
    int comparison;
    if (s1.getEdgeLabel().compareTo(s2.getEdgeLabel()) < 0) {
      comparison = -1;
    } else if (s1.getEdgeLabel().compareTo(s2.getEdgeLabel()) > 0) {
      comparison = 1;
    } else {
      if (s1.getToLabel().compareTo(s2.getToLabel()) < 0) {
        comparison = -1;
      } else if (s1.getToLabel().compareTo(s2.getToLabel()) > 0) {
        comparison = 1;
      } else {
        comparison = 0;
      }
    }
    return comparison;
  }
}
