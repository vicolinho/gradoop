package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.SearchSpaceBuilder;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;

import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos.Transaction;


import org.gradoop.model.impl.id.GradoopId;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by peet on 22.04.16.
 */
public class SearchSpace
  implements GroupReduceFunction<Tuple3<GradoopId, FatEdge, CompressedDFSCode>, Transaction> {

  private final SearchSpaceBuilder builder;

  public SearchSpace(FSMConfig fsmConfig) {
    builder = new SearchSpaceBuilder(fsmConfig);
  }

  @Override
  public void reduce(
    Iterable<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> iterable,
    Collector<Transaction> collector) throws Exception {

    Map<Integer, AdjacencyList> adjacencyLists = new HashMap<>();

    Map<CompressedDFSCode, Collection<DFSEmbedding>>
      codeEmbeddingsMap = new HashMap<>();

    builder.initAdjacencyListsAndCodeEmbeddings(
      iterable, adjacencyLists, codeEmbeddingsMap);

    collector.collect(
      new Transaction(adjacencyLists, codeEmbeddingsMap));
  }
}
