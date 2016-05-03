package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyLists;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.FilterAndRefineSearchSpaceItem;
import org.gradoop.model.impl.algorithms.fsm.tuples.IterativeSearchSpaceItem;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by peet on 22.04.16.
 */
public class FilterAndRefineSearchSpace
  extends AbstractSearchSpace
  implements GroupReduceFunction<Tuple3<GradoopId, FatEdge, CompressedDFSCode>,
  FilterAndRefineSearchSpaceItem> {

  @Override
  public void reduce(
    Iterable<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> iterable,
    Collector<FilterAndRefineSearchSpaceItem> collector) throws Exception {

    ArrayList<AdjacencyList> adjacencyLists = new ArrayList<>();
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddingsMap = new
      HashMap<>();

    fillAdjacencyListsAndCodeEmbeddings(
      iterable, adjacencyLists, codeEmbeddingsMap);

    collector.collect(
      new FilterAndRefineSearchSpaceItem(adjacencyLists, codeEmbeddingsMap));
  }
}
