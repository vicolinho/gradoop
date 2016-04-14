package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.tuples.IntegerLabeledEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.StringLabeledEdge;
import org.gradoop.model.impl.id.GradoopId;

public class EdgeLabelDecoder implements JoinFunction<
  Tuple2<GradoopId, IntegerLabeledEdge>,
  Tuple2<Integer, String>,
  Tuple2<GradoopId, StringLabeledEdge>> {


  @Override
  public Tuple2<GradoopId, StringLabeledEdge> join(
    Tuple2<GradoopId, IntegerLabeledEdge> graphIdIntegerLabeledEdge,
    Tuple2<Integer, String> dictionaryEntry) throws Exception {

    IntegerLabeledEdge integerLabeledEdge = graphIdIntegerLabeledEdge.f1;
    StringLabeledEdge stringLabeledEdge = new StringLabeledEdge();

    stringLabeledEdge.setId(integerLabeledEdge.getId());
    stringLabeledEdge.setSourceId(integerLabeledEdge.getSourceId());
    stringLabeledEdge.setTargetId(integerLabeledEdge.getTargetId());
    stringLabeledEdge.setLabel(dictionaryEntry.f1);

    return new Tuple2<>(graphIdIntegerLabeledEdge.f0, stringLabeledEdge);
  }
}
