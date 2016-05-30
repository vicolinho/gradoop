package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;

/**
 * Created by peet on 30.05.16.
 */
public class SetSupportToBranchNumber
  implements MapFunction<CompressedDFSCode, CompressedDFSCode> {

  @Override
  public CompressedDFSCode map(CompressedDFSCode compressedDFSCode) throws
    Exception {

    compressedDFSCode.setSupport(
      compressedDFSCode.getDfsCode().getMinVertexLabel());

    return compressedDFSCode;
  }
}
