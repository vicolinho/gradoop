package org.gradoop.model.impl.algorithms.fsm.tuples;

import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyListEntry;

import java.util.ArrayList;
import java.util.Collection;

public class AdjacencyList {
  private final String vertexLabel;
  private final Collection<AdjacencyListEntry> entries;

  public AdjacencyList(String vertexLabel) {
    this.vertexLabel = vertexLabel;
    entries = new ArrayList<>();
  }

  public void add(AdjacencyListEntry adjacencyListEntry) {
    this.entries.add(adjacencyListEntry);
  }

  @Override
  public String toString() {
    return vertexLabel + ":" + entries;
  }
}
