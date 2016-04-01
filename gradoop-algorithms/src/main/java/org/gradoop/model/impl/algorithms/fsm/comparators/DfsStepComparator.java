package org.gradoop.model.impl.algorithms.fsm.comparators;

import org.gradoop.model.impl.algorithms.fsm.pojos.DfsStep;

import java.io.Serializable;
import java.util.Comparator;

/**
 * implementation of GSpan lexicographic ordering
 */
public class DfsStepComparator implements Comparator<DfsStep>, Serializable {
  private final boolean directed;

  public DfsStepComparator(boolean directed) {
    this.directed = directed;
  }

  @Override
  public int compare(DfsStep s1, DfsStep s2) {
    int comparison;

    // same direction
    if (s1.isForward() == s2.isForward()) {

      // both forward
      if (s1.isForward()) {

        // starts from same position
        if (s1.getFromTime().equals(s2.getFromTime())) {

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
        if (s1.getToTime().equals(s2.getToTime())) {

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
   * extracted method to compare DFS edges based on start, edge and edge labels
   * as well as the traversal direction
   * @param s1 first DFS edge
   * @param s2 second DFS edge
   * @return comparison result
   */
  private int compareLabels(DfsStep s1, DfsStep s2) {
    int comparison;

    if (s1.getFromLabel().compareTo(s2.getFromLabel()) < 0) {
      comparison = -1;
    } else if (s1.getFromLabel().compareTo(s2.getFromLabel()) > 0 ) {
      comparison = 1;
    } else {
      if(directed) {
        comparison = compareDirectedLabels(s1, s2);
      } else {
        comparison = compareUndirectedLabels(s1, s2);
      }
    }
    return comparison;
  }

  private int compareDirectedLabels(DfsStep s1, DfsStep s2) {
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

  private int compareUndirectedLabels(DfsStep s1, DfsStep s2) {
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
