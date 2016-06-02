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
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.Supportable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * [(Supportable<CompressedDfsCode>, Support),..] => Supportable<CompressedDfsCode>[]
 */
public class CollectFrequentSubgraphs implements
  GroupReduceFunction<Supportable<CompressedDfsCode>, Collection<Supportable<CompressedDfsCode>>> {

  @Override
  public void reduce(Iterable<Supportable<CompressedDfsCode>> iterable,
    Collector<Collection<Supportable<CompressedDfsCode>>> collector) throws Exception {

    List<Supportable<CompressedDfsCode>> codes = new ArrayList<>();

    for (Supportable<CompressedDfsCode> code : iterable) {
      codes.add(code);
    }

    collector.collect(codes);
  }
}
