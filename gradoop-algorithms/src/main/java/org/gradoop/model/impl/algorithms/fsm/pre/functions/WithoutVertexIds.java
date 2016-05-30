package org.gradoop.model.impl.algorithms.fsm.pre.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTripleWithoutVertexIds;

public class WithoutVertexIds
  implements MapFunction<EdgeTriple, EdgeTripleWithoutVertexIds> {

  @Override
  public EdgeTripleWithoutVertexIds map(EdgeTriple edge) throws
    Exception {
    return new EdgeTripleWithoutVertexIds(
      edge.getGraphId(),
      edge.getEdgeLabel(),
      edge.getSourceLabel(),
      edge.getTargetLabel()
    );
  }

}
