package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyLists;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.Map;

public class BuildAdjacencyLists implements GroupReduceFunction
  <Tuple6<GradoopId, GradoopId, GradoopId, Integer, Integer, Integer>,
    AdjacencyLists> {

  @Override
  public void reduce(
    Iterable<Tuple6<GradoopId, GradoopId, GradoopId, Integer, Integer, Integer>>
      iterable, Collector<AdjacencyLists> collector) throws Exception {

    Map<GradoopId, Integer> vertexIndices = Maps.newHashMap();
    ArrayList<AdjacencyList> adjacencyLists = Lists.newArrayList();

    int edgeId = 0;

    for(Tuple6<GradoopId, GradoopId, GradoopId, Integer, Integer, Integer>
        edgeTuple : iterable) {

      Integer edgeLabel = edgeTuple.f4;

      GradoopId sourceId = edgeTuple.f1;
      Integer sourceLabel = edgeTuple.f3;
      Integer sourceIndex = vertexIndices.get(sourceId);
      AdjacencyList sourceAdjacencyList;

      if(sourceIndex == null) {
        sourceIndex = adjacencyLists.size();
        sourceAdjacencyList = new AdjacencyList(sourceLabel);
        vertexIndices.put(sourceId, sourceIndex);
        adjacencyLists.add(sourceAdjacencyList);
      } else {
        sourceAdjacencyList = adjacencyLists.get(sourceIndex);
      }

      GradoopId targetId = edgeTuple.f2;
      Integer targetLabel;
      Integer targetIndex;
      AdjacencyList targetAdjacencyList;

      if(sourceId.equals(targetId)) {
        targetLabel = sourceLabel;
        targetIndex = sourceIndex;

        targetAdjacencyList = sourceAdjacencyList;
      } else {
        targetLabel = edgeTuple.f5;
        targetIndex = vertexIndices.get(targetId);

        if(targetIndex == null) {
          targetIndex = adjacencyLists.size();
          targetAdjacencyList = new AdjacencyList(targetLabel);
          vertexIndices.put(targetId, targetIndex);
          adjacencyLists.add(targetAdjacencyList);
        } else {
          targetAdjacencyList = adjacencyLists.get(targetIndex);
        }
      }

      sourceAdjacencyList.getEntries().add(new AdjacencyListEntry(
        true, edgeId, edgeLabel, targetIndex, targetLabel));

      targetAdjacencyList.getEntries().add(new AdjacencyListEntry(
        false, edgeId, edgeLabel, sourceIndex, sourceLabel));

      edgeId++;
    }

    collector.collect(new AdjacencyLists(adjacencyLists));
  }
}
