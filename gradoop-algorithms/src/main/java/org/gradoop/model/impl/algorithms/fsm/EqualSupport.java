package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;

import java.util.ArrayList;

/**
 * Created by peet on 20.05.16.
 */
public class EqualSupport
  extends RichFlatJoinFunction<CompressedDFSCode, CompressedDFSCode, Boolean> {

  private ArrayList<String> edgeLabelDisctionary;
  private ArrayList<String> vertexLabelDisctionary;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.vertexLabelDisctionary = getRuntimeContext()
      .<ArrayList<String>>getBroadcastVariable(BroadcastNames.VERTEX_DICTIONARY)
      .get(0);
    this.edgeLabelDisctionary = getRuntimeContext()
      .<ArrayList<String>>getBroadcastVariable(BroadcastNames.EDGE_DICTIONARY)
      .get(0);
  }


  @Override
  public void join(
    CompressedDFSCode left, CompressedDFSCode right, Collector<Boolean> collector
  ) throws Exception {

    String out;

    if(right == null) {
      out = translate(left.getDfsCode()) + " " + left.getSupport() + "/-";
    } else if(left == null) {
      out = translate(right.getDfsCode()) + " -/" + right.getSupport();
    } else {
      out = translate(left.getDfsCode()) +
        " "  + left.getSupport() + "/" + right.getSupport();
    }

    boolean equal = left != null && right != null &&
      left.getSupport().equals(right.getSupport());

    if(! equal) {
      System.out.println(out);
    }

    collector.collect(equal);
  }

  private String translate(DFSCode dfsCode) {
    StringBuilder builder = new StringBuilder();

    builder.append("[");

    for(DFSStep step : dfsCode.getSteps()) {
      int fromTime = step.getFromTime();
      String fromLabel = vertexLabelDisctionary.get(step.getFromLabel());

      builder.append("(" + fromTime + ":" + fromLabel + ")");

      boolean outgoing = step.isOutgoing();
      String edgeLabel = edgeLabelDisctionary.get(step.getEdgeLabel());

      if(! outgoing) {
        builder.append("<");
      }

      builder.append("-" + edgeLabel + "-");

      if(outgoing) {
        builder.append(">");
      }

      int toTime = step.getToTime();
      String toLabel = vertexLabelDisctionary.get(step.getToLabel());

      builder.append("(" + toTime + ":" + toLabel + ")");

    }

    builder.append("]");

    return builder.toString();
  }
}
