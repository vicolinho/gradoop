package org.gradoop.model.impl.algorithms.fsm.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.id.GradoopId;


public interface TransactionalFSMCore {

  DataSet<CompressedDFSCode> mine(
    DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> fatEdges);

  void setFsmConfig(FSMConfig fsmConfig);

  void setMinSupport(DataSet<Integer> minSupport);

  void setExecutionEnvironment(ExecutionEnvironment executionEnvironment);
}
