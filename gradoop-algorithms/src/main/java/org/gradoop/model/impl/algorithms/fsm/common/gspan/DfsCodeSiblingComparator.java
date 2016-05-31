package org.gradoop.model.impl.algorithms.fsm.common.gspan;

import org.gradoop.model.impl.algorithms.fsm.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSStep;

import java.io.Serializable;
import java.util.Comparator;

public class DfsCodeSiblingComparator
  implements Comparator<DfsCode>, Serializable {

  private final boolean directed;

  public DfsCodeSiblingComparator(boolean directed) {
    this.directed = directed;
  }

  @Override
  public int compare(DfsCode c1, DfsCode c2) {

    DFSStep e1 = c1.getLastStep();
    DFSStep e2 = c2.getLastStep();

    int comparison;

    if (e1.isForward()) {
      if (e2.isBackward()) {
        // forward - backward
        comparison = 1;
      } else {
        // forward - forward
        if (e1.getFromTime() == e2.getFromTime()) {
          // forward from same vertex
          comparison = compareLabels(e1, e2);
        } else if (e1.getFromTime() > e2.getFromTime()) {
          // first forward from later vertex
          comparison = -1;
        } else  {
          // second forward from later vertex
          comparison = 1;
        }
      }
    } else {
      if(e2.isForward()) {
        // backward - forward
        comparison = -1;
      } else {
        // backward - backward
        if (e1.getToTime() == e2.getToTime()) {
          // backward to same vertex
          comparison = compareLabels(e1, e2);
        } else if (e1.getToTime() < e2.getToTime()) {
          // first back to earlier vertex
          comparison = -1;
        } else {
          // second back to earlier vertex
          comparison = 1;
        }
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
