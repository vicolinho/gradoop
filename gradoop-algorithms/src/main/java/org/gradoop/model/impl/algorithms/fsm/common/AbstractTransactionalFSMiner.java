package org.gradoop.model.impl.algorithms.fsm.common;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.common.functions.Frequent;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.functions.join.LeftSide;
import org.gradoop.model.impl.functions.tuple.Project3To0And2;
import org.gradoop.model.impl.functions.tuple.Value1Of2;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 17.05.16.
 */
public abstract class AbstractTransactionalFSMiner
  implements TransactionalFSMiner {

  /**
   * maximum iteration, if no maximum edge count provided
   */
  public static final int MAX_EDGE_COUNT = 100;
  protected ExecutionEnvironment env;

  protected DataSet<CompressedDFSCode> find1EdgeFrequentDfsCodes(
    DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> graphEdges,
    DataSet<Integer> minSupport) {

    return graphEdges
      .map(new Project3To0And2<GradoopId, FatEdge, CompressedDFSCode>())
      .distinct()
      .map(new Value1Of2<GradoopId, CompressedDFSCode>())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent())
      .withBroadcastSet(minSupport, BroadcastNames.MIN_SUPPORT);
  }

  protected DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>>
  filterFatEdges(
    DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> fatEdges,
    DataSet<CompressedDFSCode> allFrequentDfsCodes) {

    return fatEdges
      .join(allFrequentDfsCodes)
      .where("2.0").equalTo(0)
      .with(new LeftSide<Tuple3<GradoopId, FatEdge, CompressedDFSCode>, CompressedDFSCode>());
  }

  @Override
  public void setExecutionEnvironment(ExecutionEnvironment env) {
    this.env = env;
  }
}
