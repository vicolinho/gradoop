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

package org.gradoop.model.impl.algorithms.fsm.iterative.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;

import java.util.Collection;
import java.util.List;

/**
 * Core of gSpan implementation. Grows embeddings of KnownToBeGloballyFrequent DFS codes.
 */
public class GrowFrequentSubgraphs
  extends RichMapFunction<IterationItem, IterationItem> {

  private final FSMConfig fsmConfig;
  /**
   * frequent DFS codes
   */
  private Collection<CompressedDFSCode> frequentDfsCodes;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    List<Collection<CompressedDFSCode>> broadcast = getRuntimeContext()
      .getBroadcastVariable(BroadcastNames.FREQUENT_SUBGRAPHS);

    if (broadcast.isEmpty()) {
      this.frequentDfsCodes = null;
    } else {
      this.frequentDfsCodes = broadcast.get(0);
    }
  }
  /**
   * constructor
   * @param fsmConfig configuration
   */
  public GrowFrequentSubgraphs(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public IterationItem map(IterationItem wrapper) throws Exception {

    if (! wrapper.isCollector()) {
      GSpan.growFrequentSubgraphs(
        wrapper.getTransaction(), frequentDfsCodes, fsmConfig);
    }

    return wrapper;
  }
}
