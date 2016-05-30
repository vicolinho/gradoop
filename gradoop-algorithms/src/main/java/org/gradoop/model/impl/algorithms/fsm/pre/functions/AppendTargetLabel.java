package org.gradoop.model.impl.algorithms.fsm.pre.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTripleWithoutTargetLabel;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.VertexIdLabel;

public class AppendTargetLabel implements JoinFunction
  <EdgeTripleWithoutTargetLabel, VertexIdLabel, EdgeTriple> {

  @Override
  public EdgeTriple join(
    EdgeTripleWithoutTargetLabel edge, VertexIdLabel target) throws Exception {
    return new EdgeTriple(
      edge.getGraphId(),
      edge.getSourceId(),
      edge.getTargetId(),
      edge.getEdgeLabel(),
      edge.getSourceLabel(),
      target.getLabel()
    );
  }
}
