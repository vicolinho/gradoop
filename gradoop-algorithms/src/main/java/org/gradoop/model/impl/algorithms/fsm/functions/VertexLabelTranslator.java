package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.tuples.LabeledVertex;
import org.gradoop.model.impl.id.GradoopId;

public class VertexLabelTranslator
  <IL extends Comparable<IL>, OL extends Comparable<OL>>
  implements JoinFunction
  <Tuple2<GradoopId, LabeledVertex<IL>>, Tuple2<IL,OL>, Tuple2<GradoopId,
    LabeledVertex<OL>>>
{


  @Override
  public Tuple2<GradoopId, LabeledVertex<OL>> join(
    Tuple2<GradoopId, LabeledVertex<IL>> graphIdLabeledVertex,
    Tuple2<IL, OL> dictionaryEntry) throws Exception {

    GradoopId graphId = graphIdLabeledVertex.f0;
    LabeledVertex<IL> inVertex = graphIdLabeledVertex.f1;

    LabeledVertex<OL> outVertex =
      new LabeledVertex<>(inVertex.getId(), dictionaryEntry.f1);

    return new Tuple2<>(graphId, outVertex);
  }
}
