package org.gradoop.model.impl.algorithms.fsm.pojos;

import org.gradoop.model.impl.algorithms.fsm.tuples.FatEdge;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by peet on 22.04.16.
 */
public class SearchSpacePartition implements Serializable {

  private final ArrayList<ArrayList<FatEdge>> partition;

  public SearchSpacePartition(ArrayList<ArrayList<FatEdge>> partition) {
    this.partition = partition;
  }

  public ArrayList<ArrayList<FatEdge>> getPartition() {
    return partition;
  }
}
