package org.gradoop.model.impl.algorithms.fsm.common.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.IntegerLabeledEdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.VertexIdLabel;
import org.gradoop.model.impl.id.GradoopId;

public class AppendTargetLabelAndInitialDfsCode implements JoinFunction<
  Tuple5<GradoopId, GradoopId, GradoopId, Integer, Integer>,
  VertexIdLabel, Tuple3<GradoopId, IntegerLabeledEdgeTriple, CompressedDFSCode>> {

  @Override
  public Tuple3<GradoopId, IntegerLabeledEdgeTriple, CompressedDFSCode>
  join(Tuple5<GradoopId, GradoopId, GradoopId, Integer, Integer> edge,
    VertexIdLabel targetVertex) throws Exception {

    GradoopId sourceId = edge.f1;
    GradoopId targetId = edge.f2;

    Integer edgeLabel = edge.f3;
    Integer sourceLabel = edge.f4;
    Integer targetLabel = targetVertex.f1;

    int fromTime = 0;
    int toTime;


    if(sourceId.equals(targetId)) {
      toTime = fromTime;
    } else {
      toTime = 1;
    }


    DFSStep dfsStep = new DFSStep(
      fromTime, sourceLabel, true, edgeLabel, toTime, targetLabel);

    IntegerLabeledEdgeTriple triple = new IntegerLabeledEdgeTriple(
      sourceId, sourceLabel, edgeLabel, targetId, targetLabel);

    return new Tuple3<>(
      edge.f0,
      triple,
      new CompressedDFSCode(new DFSCode(dfsStep)));
  }
}
