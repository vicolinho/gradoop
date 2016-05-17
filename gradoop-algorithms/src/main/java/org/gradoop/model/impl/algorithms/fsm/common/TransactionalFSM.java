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

package org.gradoop.model.impl.algorithms.fsm.common;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMDecoder;
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMEncoder;
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.FatEdge;
import org.gradoop.model.impl.id.GradoopId;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 *
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public abstract class TransactionalFSM
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToCollectionOperator<G, V, E> {

  /**
   * frequent subgraph mining configuration
   */
  protected final FSMConfig fsmConfig;

  final TransactionalFSMEncoder<GraphCollection<G, V, E>> encoder;
  protected final TransactionalFSMiner miner;
  TransactionalFSMDecoder<GraphCollection<G, V, E>> decoder;

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   * @param miner
   */
  protected TransactionalFSM(FSMConfig fsmConfig, TransactionalFSMiner miner) {
    this.fsmConfig = fsmConfig;
    this.encoder = new GradoopTransactionalFSMEncoder<>();
    this.miner = miner;
  }

  @Override
  public GraphCollection<G, V, E> execute(
    GraphCollection<G, V, E> collection)  {

    decoder = new GradoopTransactionalFSMDecoder<>(collection.getConfig());

    DataSet<Tuple3<GradoopId, FatEdge, CompressedDFSCode>> fatEdges =
      encoder.encode(collection, fsmConfig);

    DataSet<CompressedDFSCode> frequentDfsCodes = miner
      .mine(fatEdges, encoder.getMinSupport(), fsmConfig);

    return decoder.decode(
      frequentDfsCodes,
      encoder.getVertexLabelDictionary(),
      encoder.getEdgeLabelDictionary()
    );
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
