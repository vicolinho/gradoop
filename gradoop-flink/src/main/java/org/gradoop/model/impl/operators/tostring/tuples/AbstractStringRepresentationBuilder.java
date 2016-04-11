package org.gradoop.model.impl.operators.tostring.tuples;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.model.impl.operators.tostring.api.VertexToString;

/**
 * Created by peet on 11.04.16.
 */
public abstract class AbstractStringRepresentationBuilder
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphCollectionToValueOperator<G, V, E, String> {

  /**
   * function describing string representation of graph heads
   */
  protected final GraphHeadToString<G> graphHeadToString;
  /**
   * function describing string representation of vertices
   */
  protected final VertexToString<V> vertexToString;
  /**
   * function describing string representation of edges
   */
  protected final EdgeToString<E> egeLabelingFunction;

  /**
   * constructor
   * @param graphHeadToString representation of graph heads
   * @param vertexToString representation of vertices
   * @param egeLabelingFunction representation of edges
   */
  public AbstractStringRepresentationBuilder(
    GraphHeadToString<G> graphHeadToString,
    VertexToString<V> vertexToString,
    EdgeToString<E> egeLabelingFunction) {
    this.graphHeadToString = graphHeadToString;
    this.vertexToString = vertexToString;
    this.egeLabelingFunction = egeLabelingFunction;
  }

}
