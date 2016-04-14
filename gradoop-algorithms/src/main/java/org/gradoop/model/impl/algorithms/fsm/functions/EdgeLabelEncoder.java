package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.tuples.IntegerLabeledEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.StringLabeledEdge;
import org.gradoop.model.impl.id.GradoopId;

public class EdgeLabelEncoder implements JoinFunction<
  Tuple2<GradoopId, StringLabeledEdge>,
  Tuple2<String, Integer>,
  Tuple2<GradoopId, IntegerLabeledEdge>
  > {

  @Override
  public Tuple2<GradoopId, IntegerLabeledEdge> join(
    Tuple2<GradoopId, StringLabeledEdge> graphIdStringLabeledEdge,
    Tuple2<String, Integer> dictionaryEntry) throws Exception {

    StringLabeledEdge str = graphIdStringLabeledEdge.f1;
    IntegerLabeledEdge integerLabeledEdge = new IntegerLabeledEdge();

    integerLabeledEdge.setId(str.getId());
    integerLabeledEdge.setSourceId(str.getSourceId());
    integerLabeledEdge.setTargetId(str.getTargetId());
    integerLabeledEdge.setLabel(dictionaryEntry.f1);

    return new Tuple2<>(graphIdStringLabeledEdge.f0, integerLabeledEdge);  }
}
