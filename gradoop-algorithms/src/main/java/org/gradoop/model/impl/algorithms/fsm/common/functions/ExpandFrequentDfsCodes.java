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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.SerializedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.ObjectWithCount;
import org.gradoop.model.impl.algorithms.fsm.iterative.tuples.IterationItem;


/**
 * Collector => [CompressedDfsCode,..]
 */
public class ExpandFrequentDfsCodes implements
  FlatMapFunction<IterationItem, ObjectWithCount<CompressedSubgraph>> {

  @Override
  public void flatMap(IterationItem iterationItem,
    Collector<ObjectWithCount<CompressedSubgraph>> collector) throws Exception {


    for (ObjectWithCount<CompressedSubgraph> compressedDfsCode :
      iterationItem.getFrequentSubgraphs()) {

      collector.collect(compressedDfsCode);
    }
  }
}
