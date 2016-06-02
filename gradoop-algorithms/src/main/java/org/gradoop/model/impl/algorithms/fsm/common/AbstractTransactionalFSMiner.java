package org.gradoop.model.impl.algorithms.fsm.common;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.common.functions.SearchSpace;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.GSpanTransaction;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTripleWithoutTargetLabel;
import org.gradoop.model.impl.functions.utils.LeftSide;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 17.05.16.
 */
public abstract class AbstractTransactionalFSMiner
  implements TransactionalFSMiner {

  /**
   * maximum iteration, if no maximum edge count provided
   */
  protected ExecutionEnvironment env;

  protected FSMConfig fsmConfig;


  protected DataSet<Tuple3<GradoopId, EdgeTripleWithoutTargetLabel, CompressedDfsCode>> frequent(
    DataSet<Tuple3<GradoopId, EdgeTripleWithoutTargetLabel, CompressedDfsCode>> fatEdges,
    DataSet<CompressedDfsCode> allFrequentDfsCodes) {

    return fatEdges
      .join(allFrequentDfsCodes)
      .where(2).equalTo(0)
      .with(new LeftSide<Tuple3<GradoopId, EdgeTripleWithoutTargetLabel, CompressedDfsCode>, CompressedDfsCode>());
  }

  @Override
  public void setExecutionEnvironment(ExecutionEnvironment env) {
    this.env = env;
  }

  public void setFsmConfig(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  protected DataSet<GSpanTransaction> createTransactions(
    DataSet<EdgeTriple> edges) {
    return edges
      .groupBy(0)
      .reduceGroup(new SearchSpace());
  }
}
