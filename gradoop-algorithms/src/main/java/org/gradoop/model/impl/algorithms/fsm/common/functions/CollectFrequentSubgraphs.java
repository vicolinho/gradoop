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
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.ObjectWithCount;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * [(Supportable<CompressedDfsCode>, Support),..] => Supportable<CompressedDfsCode>[]
 */
public class CollectFrequentSubgraphs implements
  GroupReduceFunction<ObjectWithCount<CompressedSubgraph>, Collection<ObjectWithCount<CompressedSubgraph>>> {


  private final FSMConfig fsmConfig;

  public CollectFrequentSubgraphs(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void reduce(Iterable<ObjectWithCount<CompressedSubgraph>> iterable,
    Collector<Collection<ObjectWithCount<CompressedSubgraph>>> collector) throws Exception {

    List<ObjectWithCount<CompressedSubgraph>> codes = new ArrayList<>();

    Iterator<ObjectWithCount<CompressedSubgraph>> iterator = iterable
      .iterator();

    if (iterator.hasNext()) {
      ObjectWithCount<CompressedSubgraph> code = iterator.next();

      if (code.getObject().getDfsCode().size() >= fsmConfig.getMinEdgeCount()) {
        codes.add(code);

        while (iterator.hasNext()) {
          codes.add(iterator.next());
        }
      }
    }


    collector.collect(codes);
  }
}
