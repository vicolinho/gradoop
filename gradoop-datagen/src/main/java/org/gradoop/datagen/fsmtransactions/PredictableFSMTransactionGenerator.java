package org.gradoop.datagen.fsmtransactions;

import org.apache.flink.api.java.DataSet;
import org.gradoop.datagen.fsmtransactions.functions.PredictableTransaction;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.CollectionGenerator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransaction;
import org.gradoop.util.GradoopFlinkConfig;

public class PredictableFSMTransactionGenerator
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements CollectionGenerator<G, V, E> {

  /**
   * Gradoop configuration
   */
  private final GradoopFlinkConfig<G, V, E> gradoopConfig;
  private final long graphCount;

  public PredictableFSMTransactionGenerator(
    GradoopFlinkConfig<G, V, E> gradoopConfig, long graphCount) {
    this.gradoopConfig = gradoopConfig;
    this.graphCount = graphCount;
  }

  @Override
  public GraphCollection<G, V, E> execute() {

    DataSet<Long> seeds = gradoopConfig
      .getExecutionEnvironment()
      .generateSequence(1, graphCount);

    DataSet<GraphTransaction<G, V, E>> transactions = seeds
      .map(new PredictableTransaction<>(
        gradoopConfig.getGraphHeadFactory(),
        gradoopConfig.getVertexFactory(),
        gradoopConfig.getEdgeFactory()
      ));

    return GraphCollection
      .fromTransactions(transactions, gradoopConfig);
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
