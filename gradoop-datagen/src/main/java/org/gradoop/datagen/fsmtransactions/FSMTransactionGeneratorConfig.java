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

  public FSMTransactionGeneratorConfig(int graphCount, int minVertexCount,
    int maxVertexCount, int minEdgeCount, int maxEdgeCount) {
    this.graphCount = graphCount;

    vertexDistribution = Distributions.LINEAR;
    this.minVertexCount = minVertexCount;
    this.maxVertexCount = maxVertexCount;

    edgeDistribution = Distributions.LINEAR;
    this.minEdgeCount = minEdgeCount;
    this.maxEdgeCount = maxEdgeCount;
  }

  public long getGraphCount() {
    return graphCount;
  }

  public int getMinVertexCount() {
    return minVertexCount;
  }

  public int getMaxVertexCount() {
    return maxVertexCount;
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



  public class Distributions {
    public static final int LINEAR = 0;
  }
}
