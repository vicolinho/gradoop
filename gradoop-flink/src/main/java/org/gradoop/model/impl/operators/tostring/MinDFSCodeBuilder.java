///*
// * This file is part of Gradoop.
// *
// * Gradoop is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * Gradoop is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
// */
//
//package org.gradoop.model.impl.operators.tostring;
//
//import org.apache.flink.api.java.DataSet;
//import org.gradoop.model.api.EPGMEdge;
//import org.gradoop.model.api.EPGMGraphHead;
//import org.gradoop.model.api.EPGMVertex;
//import org.gradoop.model.api.operators.UnaryGraphCollectionToValueOperator;
//import org.gradoop.model.impl.GraphCollection;
//import org.gradoop.model.impl.operators.tostring.functions
//  .EPGMElementToDataString;
//
///**
// * Operator deriving a string representation from a graph collection.
// * The representation follows the concept of a canonical adjacency matrix.
// * @param <G> graph type
// * @param <V> vertex type
// * @param <E> edge type
// */
//public class MinDFSCodeBuilder
//  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
//  implements UnaryGraphCollectionToValueOperator<G, V, E, String> {
//
//  /**
//   * function describing string representation of graph heads
//   */
//  protected final EPGMElementToDataString<G> graphHeadToString;
//  /**
//   * function describing string representation of vertices
//   */
//  protected final EPGMElementToDataString<V> vertexToString;
//  /**
//   * function describing string representation of edges
//   */
//  protected final EPGMElementToDataString<E> egeLabelingFunction;
//
//  /**
//   * constructor
//   * @param graphHeadToString representation of graph heads
//   * @param vertexToString representation of vertices
//   * @param egeLabelingFunction representation of edges
//   */
//  public MinDFSCodeBuilder(
//    EPGMElementToDataString<G> graphHeadToString,
//    EPGMElementToDataString<V> vertexToString,
//    EPGMElementToDataString<E> egeLabelingFunction) {
//    this.graphHeadToString = graphHeadToString;
//    this.vertexToString = vertexToString;
//    this.egeLabelingFunction = egeLabelingFunction;
//  }
//
//  @Override
//  public DataSet<String> execute(GraphCollection<G, V, E> collection) {
//
//    return collection
//      .toTransactions()
//      .map(new MinDFSCode<>(
//        graphHeadToString, vertexToString, egeLabelingFunction));
//  }
//}
