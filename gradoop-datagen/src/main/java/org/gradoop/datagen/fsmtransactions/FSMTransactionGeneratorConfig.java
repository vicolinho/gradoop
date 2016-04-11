package org.gradoop.datagen.fsmtransactions;

import java.io.Serializable;

public class FSMTransactionGeneratorConfig implements Serializable {
  private final int graphCount;
  private final int minVertexCount;
  private final int maxVertexCount;
  private final int vertexDistribution;
  private final int edgeDistribution;
  private final int minEdgeCount;
  private final int maxEdgeCount;
  private final int vertexLabelCount;
  private final int vertexLabelSize;
  private final int edgeLabelCount;
  private final int edgeLabelSize;

  public FSMTransactionGeneratorConfig(int graphCount, int minVertexCount,
    int maxVertexCount, int minEdgeCount, int maxEdgeCount,
    int vertexLabelCount, int vertexLabelSize, int edgeLabelCount,
    int edgeLabelSize) {
    this.graphCount = graphCount;


    vertexDistribution = Distributions.LINEAR;
    this.minVertexCount = minVertexCount;
    this.maxVertexCount = maxVertexCount;

    edgeDistribution = Distributions.LINEAR;
    this.minEdgeCount = minEdgeCount;
    this.maxEdgeCount = maxEdgeCount;

    this.vertexLabelCount = vertexLabelCount;
    this.vertexLabelSize = vertexLabelSize;

    this.edgeLabelCount = edgeLabelCount;
    this.edgeLabelSize = edgeLabelSize;
  }

  public long getGraphCount() {
    return graphCount;
  }

  public int calculateVertexCount(Long x, Long n) {
    return calculateCount(x, n,
      vertexDistribution, minVertexCount, maxVertexCount);
  }

  public int calculateEdgeCount(Long x, Long n) {
    return calculateCount(x, n,
      edgeDistribution, minEdgeCount, maxEdgeCount);
  }

  private int calculateCount(
    Long x, Long n, int distribution, int min, int max) {

    int count = 0;

    if(distribution == Distributions.LINEAR) {
      count = (int) (min + Math.round((double) x / n * (max - min)));
    }

    return count;
  }

  public int getVertexLabelCount() {
    return vertexLabelCount;
  }

  public int getVertexLabelSize() {
    return vertexLabelSize;
  }

  public int getEdgeLabelCount() {
    return edgeLabelCount;
  }

  public int getEdgeLabelSize() {
    return edgeLabelSize;
  }

  public class Distributions {
    public static final int LINEAR = 0;
  }
}
