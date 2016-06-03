package org.gradoop.model.impl.algorithms.fsm.common;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.SerializedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.WithCount;

public class PrintDfsCode extends RichMapFunction<WithCount<SerializedSubgraph>, String> {

  private DfsCodeTranslator translator;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    translator = new DfsCodeTranslator(getRuntimeContext());
  }


  @Override
  public String map(WithCount<SerializedSubgraph> subgraph) throws Exception {
    return subgraph.getSupport() + ";" +
      translator.translate(subgraph.getObject().getDfsCode())
//      + subgraph.getDfsCode()
      ;
  }
}
