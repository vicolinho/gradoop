package org.gradoop.model.impl.algorithms.fsm.common.pojos;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTripleWithoutTargetLabel;

import java.io.Serializable;
import java.util.ArrayList;

public class SearchSpacePartition implements Serializable {

  private final ArrayList
    <ArrayList<Tuple2<EdgeTripleWithoutTargetLabel, CompressedDFSCode>>> partition;

  public SearchSpacePartition(
    ArrayList<ArrayList<Tuple2<EdgeTripleWithoutTargetLabel, CompressedDFSCode>>> partition) {

    this.partition = partition;
  }

  public ArrayList<ArrayList<Tuple2<EdgeTripleWithoutTargetLabel, CompressedDFSCode>>> getGraphs()
  {
    return partition;
  }
}
