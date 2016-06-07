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

package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.config.FsmConfig;
import org.gradoop.model.impl.algorithms.fsm.decoders.TransactionalFsmDecoder;
import org.gradoop.model.impl.algorithms.fsm.encoders.TransactionalFsmEncoder;
import org.gradoop.model.impl.algorithms.fsm.miners.TransactionalFsMiner;

import org.gradoop.model.impl.algorithms.fsm.config.TransactionalFsmAlgorithm;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedSubgraph;
import org.gradoop.model.impl.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.decoders.gspan.GSpanGraphCollectionDecoder;
import org.gradoop.model.impl.algorithms.fsm.encoders.GraphCollectionTFsmEncoder;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.GSpanFilterRefine;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.GSpanBulkIteration;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 *
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class TransactionalFsm
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToCollectionOperator<G, V, E> {

  /**
   * frequent subgraph mining configuration
   */
  protected final FsmConfig fsmConfig;
  /**
   * input encoder (pre processing)
   */
  private final TransactionalFsmEncoder<GraphCollection<G, V, E>> encoder;
  /**
   * FSM implementation (actual algorithm)
   */
  private final TransactionalFsMiner miner;

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   * @param algorithm FSM implementation
   */
  public TransactionalFsm(FsmConfig fsmConfig, TransactionalFsmAlgorithm
    algorithm) {
    this.fsmConfig = fsmConfig;
    this.encoder = new GraphCollectionTFsmEncoder<>();
    this.miner = getMiner(algorithm);
  }

  @Override
  public GraphCollection<G, V, E> execute(
    GraphCollection<G, V, E> collection)  {

    miner.setExecutionEnvironment(
      collection.getConfig().getExecutionEnvironment());

    TransactionalFsmDecoder<GraphCollection<G, V, E>> decoder =
      new GSpanGraphCollectionDecoder<>(collection.getConfig());

    DataSet<EdgeTriple> edges = encoder.encode(collection, fsmConfig);

    DataSet<WithCount<CompressedSubgraph>> frequentDfsCodes = miner
      .mine(edges, encoder.getMinFrequency(), fsmConfig);

    return decoder.decode(
      frequentDfsCodes,
      encoder.getVertexLabelDictionary(),
      encoder.getEdgeLabelDictionary()
    );
  }

  /**
   * returns FSM implementation by a given enum
   * @param algorithm enum
   * @return FSM implementation
   */
  private TransactionalFsMiner getMiner(TransactionalFsmAlgorithm algorithm) {
    TransactionalFsMiner algorithmMiner = null;

    switch (algorithm) {
    case GSPAN_BULKITERATION: algorithmMiner = new GSpanBulkIteration();
      break;
    case GSPAN_FILTERREFINE: algorithmMiner = new GSpanFilterRefine();
      break;
    }

    return algorithmMiner;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
