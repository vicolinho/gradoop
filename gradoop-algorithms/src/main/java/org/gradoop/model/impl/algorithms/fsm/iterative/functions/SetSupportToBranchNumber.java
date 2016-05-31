package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;

/**
 * Created by peet on 30.05.16.
 */
public class SetSupportToBranchNumber
  implements MapFunction<CompressedDfsCode, CompressedDfsCode> {

  @Override
  public CompressedDfsCode map(CompressedDfsCode subgraph) throws
    Exception {

    subgraph.setMinVertexLabel(
      subgraph.getDfsCode().getMinVertexLabel());

    return subgraph;
  }
}
