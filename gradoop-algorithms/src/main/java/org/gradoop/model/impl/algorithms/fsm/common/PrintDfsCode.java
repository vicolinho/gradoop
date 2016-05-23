package org.gradoop.model.impl.algorithms.fsm.common;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;

public class PrintDfsCode extends RichMapFunction<CompressedDFSCode, String> {

  private DfsCodeTranslator translator;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    translator = new DfsCodeTranslator(getRuntimeContext());
  }


  @Override
  public String map(CompressedDFSCode compressedDFSCode) throws Exception {
    return compressedDFSCode.getSupport() + ";" +
      translator.translate(compressedDFSCode.getDfsCode());
  }
}
