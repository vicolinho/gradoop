package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.tuples.LabeledEdge;
import org.gradoop.model.impl.id.GradoopId;

public class EdgeLabelTranslator
  <IL extends Comparable<IL>, OL extends Comparable<OL>>
  implements JoinFunction
  <Tuple2<GradoopId, LabeledEdge<IL>>, Tuple2<IL,OL>, Tuple2<GradoopId,
    LabeledEdge<OL>>>
{

  @Override
  public Tuple2<GradoopId, LabeledEdge<OL>> join(
    Tuple2<GradoopId, LabeledEdge<IL>> graphIdLabeledEdge,
    Tuple2<IL, OL> dictionaryEntry) throws Exception {

    GradoopId graphId = graphIdLabeledEdge.f0;
    LabeledEdge<IL> inEdge = graphIdLabeledEdge.f1;

    LabeledEdge<OL> outEdge = new LabeledEdge<>(inEdge.getId(),
      inEdge.getSourceId(), inEdge.getTargetId(), dictionaryEntry.f1);

    return new Tuple2<>(graphId, outEdge);
  }
}
