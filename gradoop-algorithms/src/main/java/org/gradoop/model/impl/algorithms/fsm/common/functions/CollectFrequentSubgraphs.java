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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.ObjectWithCount;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * [(Supportable<CompressedDfsCode>, Support),..] => Supportable<CompressedDfsCode>[]
 */
public class CollectFrequentSubgraphs implements
  GroupReduceFunction<ObjectWithCount<CompressedSubgraph>, Collection<ObjectWithCount<CompressedSubgraph>>> {

  @Override
  public void reduce(Iterable<ObjectWithCount<CompressedSubgraph>> iterable,
    Collector<Collection<ObjectWithCount<CompressedSubgraph>>> collector) throws Exception {

    List<ObjectWithCount<CompressedSubgraph>> codes = new ArrayList<>();

    for (ObjectWithCount<CompressedSubgraph> code : iterable) {
      codes.add(code);
    }

    collector.collect(codes);
  }
}
