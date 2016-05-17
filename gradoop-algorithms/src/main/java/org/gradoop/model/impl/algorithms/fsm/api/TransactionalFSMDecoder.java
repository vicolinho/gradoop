package org.gradoop.model.impl.algorithms.fsm.api;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;

import java.util.ArrayList;

public interface TransactionalFSMDecoder<T> {

  T decode(
    DataSet<CompressedDFSCode> frequentDfsCodes,
    DataSet<ArrayList<String>> vertexLabelDictionary,
    DataSet<ArrayList<String>> edgeLabelDictionary
  );
}
