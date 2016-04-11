package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Collection;
import java.util.Set;

public class GraphTransaction
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends Tuple3<G, Set<V>, Set<E>> {

  public GraphTransaction() {

  }

  public GraphTransaction(G graphHead, Set<V> vertices, Set<E> edges) {
    setGraphHead(graphHead);
    setVertices(vertices);
    setEdges(edges);
  }

  public G getGraphHead() {
    return this.f0;
  }

  public void setGraphHead(G graphHead) {
    this.f0 = graphHead;
  }

  public Set<V> getVertices() {
    return this.f1;
  }

  public void setVertices(Set<V> vertices) {
    this.f1 = vertices;
  }

  public Set<E> getEdges() {
    return this.f2;
  }

  public void  setEdges(Set<E> edges) {
    this.f2 = edges;
  }

  public Collection<E> getEdges(GradoopId vertexId) {
    Collection<E> edges = Lists.newArrayList();

    for(E edge : getEdges()) {
      if(edge.getSourceId().equals(vertexId) ||
        edge.getTargetId().equals(vertexId)) {

        edges.add(edge);
      }
    }

    return edges;
  }
}
