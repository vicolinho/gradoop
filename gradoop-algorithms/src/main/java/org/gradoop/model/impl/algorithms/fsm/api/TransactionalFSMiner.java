package org.gradoop.model.impl.algorithms.fsm.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.Supportable;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTriple;


public interface TransactionalFSMiner {

  DataSet<Supportable<CompressedDfsCode>> mine(DataSet<EdgeTriple> edges,
    DataSet<Integer> minSupport, FSMConfig fsmConfig);

  void setExecutionEnvironment(ExecutionEnvironment env);
}
