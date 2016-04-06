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

package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;

import java.util.ArrayList;
import java.util.List;

/**
 * [(CompressedDFSCode, Support),..] => CompressedDFSCode[]
 */
public class ConcatFrequentDfsCodes implements GroupReduceFunction
  <Tuple2<CompressedDFSCode, Integer>, CompressedDFSCode[]> {

  @Override
  public void reduce(Iterable<Tuple2<CompressedDFSCode, Integer>> iterable,
    Collector<CompressedDFSCode[]> collector) throws Exception {

    List<CompressedDFSCode> codes = new ArrayList<>();

    for (Tuple2<CompressedDFSCode, Integer> pair : iterable) {
      codes.add(pair.f0);
    }

    collector.collect(codes.toArray(new CompressedDFSCode[codes.size()]));
  }
}
