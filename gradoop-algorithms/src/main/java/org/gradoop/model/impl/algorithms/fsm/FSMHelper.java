package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;

public class FSMHelper
{
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  void printMinDfsCodes(

    GraphCollection<G, V, E> collection, FSMConfig fsmConfig) throws Exception {

    DataSet<GraphTransaction<G, V, E>> transactions = collection
      .toTransactions();

    DataSet<CompressedDfsCode> minDfsCodes = transactions
      .map(new MinDfsCode<G, V, E>(fsmConfig));

    System.out.println("\n");
    minDfsCodes.print();
  }
}

