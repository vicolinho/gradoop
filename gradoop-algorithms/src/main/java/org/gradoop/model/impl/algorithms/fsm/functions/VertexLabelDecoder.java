package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.tuples.IntegerLabeledVertex;
import org.gradoop.model.impl.algorithms.fsm.tuples.StringLabeledVertex;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 13.04.16.
 */
public class VertexLabelDecoder implements JoinFunction<Tuple2<GradoopId,IntegerLabeledVertex>,
  Tuple2<Integer, String>, Tuple2<GradoopId,StringLabeledVertex>> {


  @Override
  public Tuple2<GradoopId, StringLabeledVertex> join(
    Tuple2<GradoopId, IntegerLabeledVertex> graphIdIntegerLabeledVertex,
    Tuple2<Integer, String> dictionaryEntry) throws Exception {

    StringLabeledVertex stringLabeledVertex = new StringLabeledVertex();

    stringLabeledVertex.setId(graphIdIntegerLabeledVertex.f1.getId());
    stringLabeledVertex.setLabel(dictionaryEntry.f1);


    return new Tuple2<>(graphIdIntegerLabeledVertex.f0, stringLabeledVertex);
  }
}
