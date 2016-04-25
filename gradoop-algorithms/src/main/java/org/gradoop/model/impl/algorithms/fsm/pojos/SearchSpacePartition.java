package org.gradoop.model.impl.algorithms.fsm.pojos;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by peet on 22.04.16.
 */
public class SearchSpacePartition implements Serializable {

  private final ArrayList<AdjacencyLists> partition;

  public SearchSpacePartition(ArrayList<AdjacencyLists> partition) {
    this.partition = partition;
  }
}
