package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.VertexIdLabel;
import org.gradoop.model.impl.id.GradoopId;

public class AppendTargetLabel implements JoinFunction<
  Tuple5<GradoopId, GradoopId, GradoopId, Integer, Integer>,
  VertexIdLabel, Tuple3<GradoopId, FatEdge, CompressedDFSCode>> {

  @Override
  public Tuple3<GradoopId, FatEdge, CompressedDFSCode>
  join(Tuple5<GradoopId, GradoopId, GradoopId, Integer, Integer> edge,
    VertexIdLabel targetVertex) throws Exception {

    GradoopId sourceId = edge.f1;
    GradoopId targetId = edge.f2;

    Integer edgeLabel = edge.f3;
    Integer sourceLabel = edge.f4;
    Integer targetLabel = targetVertex.f1;

    int fromTime = 0;
    Integer fromLabel;
    Boolean outgoing;
    int toTime;
    Integer toLabel;


    if(sourceId.equals(targetId)) {
      fromLabel = sourceLabel;
      toLabel = fromLabel;
      toTime = fromTime;
      outgoing = true;
    } else {
      toTime = 1;

      outgoing = sourceLabel.compareTo(targetLabel) <= 0;

      if(outgoing) {
        fromLabel = sourceLabel;
        toLabel = targetLabel;
      } else {
        fromLabel = targetLabel;
        toLabel = sourceLabel;
      }
    }

    DFSStep dfsStep = new DFSStep(
      fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel);

    return new Tuple3<>(
      edge.f0,
      new FatEdge(sourceId, targetId, edgeLabel, sourceLabel, targetLabel),
      new CompressedDFSCode(new DFSCode(dfsStep)));
  }
}
