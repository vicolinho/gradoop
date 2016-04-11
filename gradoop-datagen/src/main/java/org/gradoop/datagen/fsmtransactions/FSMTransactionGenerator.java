package org.gradoop.datagen.fsmtransactions;

import com.google.common.collect.Lists;
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
import scala.collection.mutable.StringBuilder;

import java.util.List;
import java.util.Random;


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
      .with(new FSMTransaction<>(
        gradoopConfig, generatorConfig, getVertexLabels(), getEdgeLabels()));

    return GraphCollection
      .fromTransactions(transactions, gradoopConfig);
  }

  private List<String> getVertexLabels() {
    int labelCount = generatorConfig.getVertexLabelCount();
    int startCharIndex = 65;
    int labelSize = generatorConfig.getVertexLabelSize();

    return getLabels(labelCount, labelSize, startCharIndex);
  }

  private List<String> getEdgeLabels() {
    int labelCount = generatorConfig.getEdgeLabelCount();
    int startCharIndex = 97;
    int labelSize = generatorConfig.getEdgeLabelSize();

    return getLabels(labelCount, labelSize, startCharIndex);
  }

  private List<String> getLabels(int labelCount, int labelSize,
    int startCharIndex) {
    List<String> labels =
      Lists.newArrayListWithCapacity(labelCount);

    Random random = new Random();

    for(int i = 0; i < labelCount; i++) {
      StringBuilder builder = new StringBuilder();

      for(int j = 0; j < labelSize; j++) {
        char c = (char) (startCharIndex + random.nextInt(26));
        builder.append(c);
      }
      labels.add(builder.toString());
    }
    return labels;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
