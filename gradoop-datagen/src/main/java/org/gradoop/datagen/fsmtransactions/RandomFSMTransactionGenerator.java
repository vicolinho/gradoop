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

package org.gradoop.datagen.fsmtransactions;

import com.google.common.collect.Lists;
import org.gradoop.datagen.fsmtransactions.functions.RandomTransaction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.CollectionGenerator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.util.GradoopFlinkConfig;
import scala.collection.mutable.StringBuilder;

import java.util.List;
import java.util.Random;

/**
 * creates a collection of random but connected graphs.
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class RandomFSMTransactionGenerator
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements CollectionGenerator<G, V, E> {

  /**
   * Gradoop configuration
   */
  private final GradoopFlinkConfig<G, V, E> gradoopConfig;
  /**
   * generator configuration
   */
  private final FSMTransactionGeneratorConfig generatorConfig;

  /**
   * constructor
   * @param gradoopConfig Gradoop configuration
   * @param generatorConfig generator configuration
   */
  public RandomFSMTransactionGenerator(GradoopFlinkConfig<G, V, E> gradoopConfig,
    FSMTransactionGeneratorConfig generatorConfig) {
    this.gradoopConfig = gradoopConfig;
    this.generatorConfig = generatorConfig;
  }

  @Override
  public GraphCollection<G, V, E> execute() {

    DataSet<Long> seeds = gradoopConfig.getExecutionEnvironment()
      .generateSequence(1, generatorConfig.getGraphCount());

    DataSet<Long> count = Count.count(seeds);

    DataSet<GraphTransaction<G, V, E>> transactions = seeds
      .cross(count)
      .with(new RandomTransaction<>(
        gradoopConfig, generatorConfig, getVertexLabels(), getEdgeLabels()));

    return GraphCollection
      .fromTransactions(transactions, gradoopConfig);
  }

  /**
   * creates a list of vertex labels
   * @return list of labels
   */
  private List<String> getVertexLabels() {
    int labelCount = generatorConfig.getVertexLabelCount();
    int startCharIndex = 65;
    int labelSize = generatorConfig.getVertexLabelSize();

    return getLabels(labelCount, labelSize, startCharIndex);
  }

  /**
   * creates a list of edge labels
   * @return list of labels
   */
  private List<String> getEdgeLabels() {
    int labelCount = generatorConfig.getEdgeLabelCount();
    int startCharIndex = 97;
    int labelSize = generatorConfig.getEdgeLabelSize();

    return getLabels(labelCount, labelSize, startCharIndex);
  }

  /**
   * creates a list of labels
   * @param labelCount number of labels
   * @param labelSize label length
   * @param startCharIndex start character ASCII index
   * @return list of labels
   */
  private List<String> getLabels(
    int labelCount, int labelSize, int startCharIndex) {

    List<String> labels = Lists.newArrayListWithCapacity(labelCount);

    Random random = new Random();

    for (int i = 0; i < labelCount; i++) {
      StringBuilder builder = new StringBuilder();

      for (int j = 0; j < labelSize; j++) {
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
