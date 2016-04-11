package org.gradoop.datagen.fsmtransactions;

import org.gradoop.datagen.fsmtransactions.functions.FSMTransaction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.CollectionGenerator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransaction;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.util.GradoopFlinkConfig;


public class FSMTransactionGenerator
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements CollectionGenerator<G, V, E> {

  private final FSMTransactionGeneratorConfig generatorConfig;
  private final GradoopFlinkConfig<G, V, E>
    gradoopConfig;

  public FSMTransactionGenerator(GradoopFlinkConfig<G, V, E> gradoopConfig,
    FSMTransactionGeneratorConfig generatorConfig) {
    this.gradoopConfig = gradoopConfig;
    this.generatorConfig = generatorConfig;
  }

  @Override
  public GraphCollection<G, V, E> execute() {

    DataSet<Long> seeds = gradoopConfig.getExecutionEnvironment()
      .generateSequence(0, generatorConfig.getGraphCount());

    DataSet<Long> count = Count.count(seeds);

    DataSet<GraphTransaction<G, V, E>> transactions = seeds
      .cross(count)
      .with(new FSMTransaction<>(gradoopConfig, generatorConfig));

    return GraphCollection
      .fromTransactions(transactions, gradoopConfig);
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
