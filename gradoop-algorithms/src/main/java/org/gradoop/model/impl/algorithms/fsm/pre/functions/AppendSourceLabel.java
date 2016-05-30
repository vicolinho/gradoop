package org.gradoop.model.impl.algorithms.fsm.pre.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTripleWithoutTargetLabel;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTripleWithoutVertexLabels;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.VertexIdLabel;

public class AppendSourceLabel
  implements JoinFunction
  <EdgeTripleWithoutVertexLabels, VertexIdLabel, EdgeTripleWithoutTargetLabel> {


  @Override
  public EdgeTripleWithoutTargetLabel join(
    EdgeTripleWithoutVertexLabels edge, VertexIdLabel source) throws Exception {
    return new EdgeTripleWithoutTargetLabel(
      edge.getGraphId(),
      edge.getSourceId(),
      edge.getTargetId(),
      edge.getEdgeLabel(),
      source.getLabel()
    );
  }
}
