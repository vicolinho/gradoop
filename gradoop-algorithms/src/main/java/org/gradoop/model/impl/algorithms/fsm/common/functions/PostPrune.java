package org.gradoop.model.impl.algorithms.fsm.common.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;

public class PostPrune
  implements FlatMapFunction<CompressedDFSCode, CompressedDFSCode> {

  private final FSMConfig fsmConfig;

  public PostPrune(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void flatMap(CompressedDFSCode compressedDFSCode,
    Collector<CompressedDFSCode> collector) throws Exception {

    if (GSpan.isValidMinimumDfsCode(compressedDFSCode, fsmConfig)) {
      collector.collect(compressedDFSCode);
    }
  }
}
