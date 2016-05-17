package org.gradoop.model.impl.algorithms.fsm.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;

public interface TransactionalFSMEncoder<T> {



  DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> encode(
    T input, FSMConfig fsmConfig);

  DataSet<Integer> getMinSupport();

  DataSet<ArrayList<String>> getVertexLabelDictionary();

  DataSet<ArrayList<String>> getEdgeLabelDictionary();
}
