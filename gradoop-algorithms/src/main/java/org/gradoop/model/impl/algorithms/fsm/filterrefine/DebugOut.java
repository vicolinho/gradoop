package org.gradoop.model.impl.algorithms.fsm.filterrefine;

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

    String prefix = "FR";
    print(prefix, compressedDFSCode);

    return compressedDFSCode;
  }

  public static void print(String prefix, CompressedDFSCode compressedDFSCode) {
    System.out.println(prefix + ";" + compressedDFSCode);
  }
}
