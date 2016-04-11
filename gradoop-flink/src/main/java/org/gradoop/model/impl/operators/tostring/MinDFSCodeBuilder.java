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

package org.gradoop.model.impl.operators.tostring;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.model.impl.operators.tostring.api.VertexToString;
import org.gradoop.model.impl.operators.tostring.tuples
  .AbstractStringRepresentationBuilder;

/**
 * Operator deriving a string representation from a graph collection.
 * The representation follows the concept of a canonical adjacency matrix.
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class MinDFSCodeBuilder
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends AbstractStringRepresentationBuilder<G, V, E> {


  /**
   * constructor
   *
   * @param graphHeadToString   representation of graph heads
   * @param vertexToString      representation of vertices
   * @param egeLabelingFunction representation of edges
   */
  public MinDFSCodeBuilder(
    GraphHeadToString<G> graphHeadToString,
    VertexToString<V> vertexToString,
    EdgeToString<E> egeLabelingFunction) {
    super(graphHeadToString, vertexToString, egeLabelingFunction);
  }

  @Override
  public DataSet<String> execute(GraphCollection<G, V, E> collection) {

    return collection
      .toTransactions()
      .map(new MinDFSCode<>(
        graphHeadToString, vertexToString, egeLabelingFunction));
  }
}
