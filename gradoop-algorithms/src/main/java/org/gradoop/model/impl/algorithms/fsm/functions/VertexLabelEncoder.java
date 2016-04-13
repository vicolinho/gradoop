package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.tuples.IntegerLabeledVertex;
import org.gradoop.model.impl.algorithms.fsm.tuples.StringLabeledVertex;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 13.04.16.
 */
public class VertexLabelEncoder
  implements JoinFunction<Tuple2<GradoopId, StringLabeledVertex>,
  Tuple2<String, Integer>, Tuple2<GradoopId, IntegerLabeledVertex>> {


  @Override
  public Tuple2<GradoopId, IntegerLabeledVertex> join(Tuple2<GradoopId,
    StringLabeledVertex> graphIdStringLabeledVertex,
    Tuple2<String, Integer> dictionaryEntry) throws Exception {

    IntegerLabeledVertex integerLabeledVertex = new IntegerLabeledVertex();

    integerLabeledVertex.setId(graphIdStringLabeledVertex.f1.getId());
    integerLabeledVertex.setLabel(dictionaryEntry.f1);

    return new Tuple2<>(graphIdStringLabeledVertex.f0, integerLabeledVertex);
  }
}
