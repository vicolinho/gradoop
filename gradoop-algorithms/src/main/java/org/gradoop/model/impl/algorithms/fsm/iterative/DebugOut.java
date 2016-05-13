package org.gradoop.model.impl.algorithms.fsm.iterative;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;

/**
 * Created by peet on 13.05.16.
 */
public class DebugOut implements
  MapFunction<CompressedDFSCode, CompressedDFSCode> {
  @Override
  public CompressedDFSCode map(CompressedDFSCode compressedDFSCode) throws
    Exception {

    if (compressedDFSCode.getDfsCode().size() > 1) {
      System.out.println("IT;" + compressedDFSCode);
    }

    return compressedDFSCode;
  }
}
