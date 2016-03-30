package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.util.GradoopFlinkConfig;

public class GSpan
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToCollectionOperator<G, V, E> {

  private final float threshold;
  protected DataSet<Long> minCount;
  private GradoopFlinkConfig<G, V, E> config;

  public GSpan(float threshold) {
    this.threshold = threshold;
  }

  @Override
  public GraphCollection<G, V, E> execute(GraphCollection<G, V, E> collection)  {

    this.config = collection.getConfig();


    return null;
  }


  @Override
  public String getName() {
    return null;
  }
}
