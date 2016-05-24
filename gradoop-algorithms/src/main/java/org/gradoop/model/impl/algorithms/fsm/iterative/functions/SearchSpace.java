/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.SearchSpaceBuilder;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.Transaction;


import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * (GraphId, [Vertex,..]) |><| (GraphId, [Edge,..]) => Graph
 */
public class SearchSpace
  implements GroupReduceFunction
  <Tuple3<GradoopId, FatEdge, CompressedDFSCode>, Transaction> {

  private final SearchSpaceBuilder builder;

  public SearchSpace(FSMConfig fsmConfig) {
    builder = new SearchSpaceBuilder(fsmConfig);
  }

  @Override
  public void reduce(
    Iterable<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> iterable,
    Collector<Transaction> collector) throws Exception {

    Map<Integer, AdjacencyList> adjacencyLists = new HashMap<>();
    HashMap<CompressedDFSCode, Collection<DFSEmbedding>>
      codeEmbeddingsMap = new HashMap<>();

    builder.initAdjacencyListsAndCodeEmbeddings(iterable, adjacencyLists,
      codeEmbeddingsMap);

    collector.collect(Transaction
      .createForGraph(adjacencyLists, codeEmbeddingsMap));
  }
}
