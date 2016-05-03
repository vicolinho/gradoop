package org.gradoop.model.impl.algorithms.fsm.naiveparallel.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.SearchSpaceBuilder;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;

import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.naiveparallel.pojos.Transaction;


import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by peet on 22.04.16.
 */
public class SearchSpace
  implements GroupReduceFunction<Tuple3<GradoopId, FatEdge, CompressedDFSCode>, Transaction> {

  private final SearchSpaceBuilder builder = new SearchSpaceBuilder();

  @Override
  public void reduce(
    Iterable<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> iterable,
    Collector<Transaction> collector) throws Exception {

    ArrayList<AdjacencyList> adjacencyLists = new ArrayList<>();

    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddingsMap = new
      HashMap<>();

    builder.initAdjacencyListsAndCodeEmbeddings(
      iterable, adjacencyLists, codeEmbeddingsMap);

    collector.collect(
      new Transaction(adjacencyLists, codeEmbeddingsMap));
  }
}
