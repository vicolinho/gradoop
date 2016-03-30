package org.gradoop.model.impl.algorithms.fsm.pojos;

import org.gradoop.model.impl.algorithms.fsm.tuples.AdjacencyList;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Map;

/**
 * Created by peet on 30.03.16.
 */
public class SearchSpaceItem {

  private final Map<GradoopId, AdjacencyList> adjacencyLists;

  public SearchSpaceItem(Map<GradoopId, AdjacencyList> adjacencyLists) {
    this.adjacencyLists = adjacencyLists;
  }

  @Override
  public String toString() {
    return "--------------------------\n" +
      adjacencyLists.values();
  }
}
