package org.gradoop.model.impl.algorithms.fsm.pojos;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.FatEdge;

import java.io.Serializable;
import java.util.ArrayList;

public class SearchSpacePartition implements Serializable {

  private final ArrayList
    <ArrayList<Tuple2<FatEdge, CompressedDFSCode>>> partition;

  public SearchSpacePartition(
    ArrayList<ArrayList<Tuple2<FatEdge, CompressedDFSCode>>> partition) {

    this.partition = partition;
  }

  public ArrayList<ArrayList<Tuple2<FatEdge, CompressedDFSCode>>> getGraphs()
  {
    return partition;
  }
}
