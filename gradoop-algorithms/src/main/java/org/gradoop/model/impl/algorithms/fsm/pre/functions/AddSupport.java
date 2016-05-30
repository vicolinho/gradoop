package org.gradoop.model.impl.algorithms.fsm.pre.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTripleWithSupport;

public class AddSupport implements JoinFunction<
  Tuple4<Integer, Integer, Integer, Integer>, EdgeTriple, EdgeTripleWithSupport>
{

  @Override
  public EdgeTripleWithSupport join(
    Tuple4<Integer, Integer, Integer, Integer> quadruple,
    EdgeTriple edge) throws Exception {
    return new EdgeTripleWithSupport(
      edge.getGraphId(),
      edge.getSourceId(),
      edge.getTargetId(),
      edge.getEdgeLabel(),
      edge.getSourceLabel(),
      edge.getTargetLabel(),
      quadruple.f3
    );
  }
}
