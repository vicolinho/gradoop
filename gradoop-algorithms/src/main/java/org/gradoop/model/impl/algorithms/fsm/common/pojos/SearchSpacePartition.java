package org.gradoop.model.impl.algorithms.fsm.common.pojos;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.IntegerLabeledEdgeTriple;

import java.io.Serializable;
import java.util.ArrayList;

public class SearchSpacePartition implements Serializable {

  private final ArrayList
    <ArrayList<Tuple2<IntegerLabeledEdgeTriple, CompressedDFSCode>>> partition;

  public SearchSpacePartition(
    ArrayList<ArrayList<Tuple2<IntegerLabeledEdgeTriple, CompressedDFSCode>>> partition) {

    this.partition = partition;
  }

  public ArrayList<ArrayList<Tuple2<IntegerLabeledEdgeTriple, CompressedDFSCode>>> getGraphs()
  {
    return partition;
  }
}
