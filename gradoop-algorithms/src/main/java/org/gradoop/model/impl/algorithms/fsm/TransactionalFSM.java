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
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMDecoder;
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMEncoder;
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common
  .GradoopTransactionalFSMDecoder;
import org.gradoop.model.impl.algorithms.fsm.common.GradoopTransactionalFSMEncoder;

import org.gradoop.model.impl.algorithms.fsm.common.TransactionalFsmAlgorithm;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.filterrefine
  .FilterRefineGSpanMiner;
import org.gradoop.model.impl.algorithms.fsm.iterative.GSpanBulkIteration;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTriple;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 *
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class TransactionalFSM
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToCollectionOperator<G, V, E> {

  /**
   * frequent subgraph mining configuration
   */
  protected final FSMConfig fsmConfig;
  /**
   * input encoder (pre processing)
   */
  private final TransactionalFSMEncoder<GraphCollection<G, V, E>> encoder;
  /**
   * FSM implementation (actual algorithm)
   */
  private final TransactionalFSMiner miner;

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   * @param algorithm FSM implementation
   */
  public TransactionalFSM(FSMConfig fsmConfig, TransactionalFsmAlgorithm
    algorithm) {
    this.fsmConfig = fsmConfig;
    this.encoder = new GradoopTransactionalFSMEncoder<>();
    this.miner = getMiner(algorithm);
  }

  @Override
  public GraphCollection<G, V, E> execute(
    GraphCollection<G, V, E> collection)  {

    TransactionalFSMDecoder<GraphCollection<G, V, E>> decoder =
      new GradoopTransactionalFSMDecoder<>(collection.getConfig());

    DataSet<EdgeTriple> edges = encoder.encode(collection, fsmConfig);

    DataSet<WithCount<CompressedSubgraph>> frequentDfsCodes = miner
      .mine(edges, encoder.getMinSupport(), fsmConfig);

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
  private TransactionalFSMiner getMiner(TransactionalFsmAlgorithm algorithm) {
    TransactionalFSMiner algorithmMiner = null;

    switch (algorithm) {
    case GSPAN_BULKITERATION: algorithmMiner = new GSpanBulkIteration();
      break;
    case GSPAN_FILTERREFINE: algorithmMiner = new FilterRefineGSpanMiner();
      break;
    }

    return algorithmMiner;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
