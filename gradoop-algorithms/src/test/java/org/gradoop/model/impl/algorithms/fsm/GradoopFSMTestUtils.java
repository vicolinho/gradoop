package org.gradoop.model.impl.algorithms.fsm;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichGroupCombineFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.DfsCodeComparator;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DfsStep;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.ObjectWithCount;

import java.util.Collections;
import java.util.List;

public class GradoopFSMTestUtils {

  public static void sortTranslateAndPrint(
    DataSet<ObjectWithCount<CompressedSubgraph>> iResult,
    DataSet<List<String>> vertexLabelDictionary,
    DataSet<List<String>> edgeLabelDictionary) throws Exception {

    System.out.println(
      iResult
        .combineGroup(new SortAndTranslate())
        .withBroadcastSet(
          vertexLabelDictionary, BroadcastNames.VERTEX_DICTIONARY)
        .withBroadcastSet(
          edgeLabelDictionary, BroadcastNames.EDGE_DICTIONARY)
        .collect()
        .get(0)
    );
  }

  private static class SortAndTranslate
    extends
    RichGroupCombineFunction<ObjectWithCount<CompressedSubgraph>, String> {

    private List<String> vertexDictionary;
    private List<String> edgeDictionary;


    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      this.vertexDictionary = getRuntimeContext()
        .<List<String>>getBroadcastVariable(BroadcastNames.VERTEX_DICTIONARY)
        .get(0);
      this.edgeDictionary = getRuntimeContext()
        .<List<String>>getBroadcastVariable(BroadcastNames.EDGE_DICTIONARY)
        .get(0);
    }

    @Override
    public void combine(Iterable<ObjectWithCount<CompressedSubgraph>> iterable,
      Collector<String> collector) throws Exception {

      List<DfsCode> subgraphs = Lists.newArrayList();
      List<String> strings = Lists.newArrayList();


      for(ObjectWithCount<CompressedSubgraph> subgraphWithCount : iterable) {

        subgraphs.add(subgraphWithCount.getObject().getDfsCode());
      }

      Collections.sort(subgraphs, new DfsCodeComparator(true));


      for(DfsCode subgraph : subgraphs) {
        int lastToTime = -1;
        StringBuilder builder = new StringBuilder();

        for(DfsStep step : subgraph.getSteps()) {
          int fromTime = step.getFromTime();
          String fromLabel = vertexDictionary.get(step.getFromLabel());
          boolean outgoing = step.isOutgoing();
          String edgeLabel = edgeDictionary.get(step.getEdgeLabel());
          int toTime = step.getToTime();
          String toLabel = vertexDictionary.get(step.getToLabel());

          if (lastToTime != fromTime) {
            builder
              .append(" ")
              .append(formatVertex(fromTime, fromLabel));
          }
          builder
            .append(formatEdge(outgoing, edgeLabel))
            .append(formatVertex(toTime, toLabel));

          lastToTime = toTime;
        }

        strings.add(builder.toString());
      }


      collector.collect(StringUtils.join(strings, "\n"));
    }


  }

  private static String formatVertex(int id, String label) {
    return "(" + id + ":" + label + ")";
  }

  private static String formatEdge(boolean outgoing, String edgeLabel) {
    return outgoing ?
      "-" + edgeLabel + "->" : "<-" + edgeLabel + "-";
  }
}
