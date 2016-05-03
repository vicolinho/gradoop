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

package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.FatEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.IterativeSearchSpaceItem;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * (GraphId, [Vertex,..]) |><| (GraphId, [Edge,..]) => Graph
 */
public class IterativeSearchSpace extends AbstractSearchSpace
  implements GroupReduceFunction
  <Tuple3<GradoopId, FatEdge, CompressedDFSCode>, IterativeSearchSpaceItem> {

  @Override
  public void reduce(
    Iterable<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> iterable,
    Collector<IterativeSearchSpaceItem> collector) throws Exception {

    ArrayList<AdjacencyList> adjacencyLists = new ArrayList<>();
    HashMap<CompressedDFSCode, HashSet<DFSEmbedding>> codeEmbeddingsMap = new
      HashMap<>();

    fillAdjacencyListsAndCodeEmbeddings(iterable, adjacencyLists,
      codeEmbeddingsMap);

    collector.collect(IterativeSearchSpaceItem
      .createForGraph(adjacencyLists, codeEmbeddingsMap));
  }
}
