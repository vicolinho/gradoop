package org.gradoop.model.impl.algorithms.fsm.common;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSStep;

import java.util.List;

public class DfsCodeTranslator {
  private final List<String> vertexLabelDictionary;
  private final List<String> edgeLabelDictionary;

  public DfsCodeTranslator(RuntimeContext runtimeContext) {
    this.vertexLabelDictionary =
      runtimeContext.<List<String>>getBroadcastVariable(
        BroadcastNames.VERTEX_DICTIONARY).get(0);
    this.edgeLabelDictionary =
      runtimeContext.<List<String>>getBroadcastVariable(
        BroadcastNames.EDGE_DICTIONARY).get(0);
  }

  public String translate(DFSCode dfsCode) {
    StringBuilder builder = new StringBuilder();

    builder.append("[");

    for(DFSStep step : dfsCode.getSteps()) {
      int fromTime = step.getFromTime();
      String fromLabel = vertexLabelDictionary.get(step.getFromLabel());

      builder.append("(" + fromTime + ":" + fromLabel + ")");

      boolean outgoing = step.isOutgoing();
      String edgeLabel = edgeLabelDictionary.get(step.getEdgeLabel());

      if(! outgoing) {
        builder.append("<");
      }

      builder.append("-" + edgeLabel + "-");

      if(outgoing) {
        builder.append(">");
      }

      int toTime = step.getToTime();
      String toLabel = vertexLabelDictionary.get(step.getToLabel());

      builder.append("(" + toTime + ":" + toLabel + ")");

    }

    builder.append("]");

    return builder.toString();
  }
}
