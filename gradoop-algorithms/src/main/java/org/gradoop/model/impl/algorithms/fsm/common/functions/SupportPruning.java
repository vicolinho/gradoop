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

package org.gradoop.model.impl.algorithms.fsm.common.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.PatternGrower;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;


import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Core of gSpan implementation. Grows embeddings of KnownToBeGloballyFrequent DFS codes.
 */
public class SupportPruning
  extends RichMapFunction<IterationItem, IterationItem> {

  /**
   * frequent DFS codes
   */
  private Collection<CompressedDFSCode> frequentDfsCodes;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    List<Collection<CompressedDFSCode>> broadcast = getRuntimeContext()
      .getBroadcastVariable(BroadcastNames.FREQUENT_PATTERNS);

    if (broadcast.isEmpty()) {
      this.frequentDfsCodes = null;
    } else {
      this.frequentDfsCodes = broadcast.get(0);
    }
  }

  @Override
  public IterationItem map(IterationItem iterationItem) throws Exception {

    if (frequentDfsCodes != null) {
      if (iterationItem.isCollector()) {
        iterationItem.collect(frequentDfsCodes);
      } else {
        PatternGrower.prune(iterationItem.getTransaction(), frequentDfsCodes);
      }
    }
    return iterationItem;
  }
}
