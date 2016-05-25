package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransaction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;

public class FSMHelper
{
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  void printMinDfsCodes(

    GraphCollection<G, V, E> collection) throws Exception {

    DataSet<GraphTransaction<G, V, E>> transactions = collection
      .toTransactions();

    DataSet<CompressedDFSCode> minDfsCodes = transactions
      .map(new MinDfsCode<G, V, E>());

    System.out.println("\n");
    minDfsCodes.print();
  }
}

