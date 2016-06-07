package org.gradoop.model.impl.algorithms.fsm.miners;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.impl.algorithms.fsm.config.FsmConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedSubgraph;
import org.gradoop.model.impl.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;


public interface TransactionalFsMiner {

  DataSet<WithCount<CompressedSubgraph>> mine(DataSet<EdgeTriple> edges,
    DataSet<Integer> minSupport, FsmConfig fsmConfig);

  void setExecutionEnvironment(ExecutionEnvironment env);
}
