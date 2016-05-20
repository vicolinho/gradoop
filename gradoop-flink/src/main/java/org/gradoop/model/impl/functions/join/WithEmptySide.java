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
package org.gradoop.model.impl.functions.join;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

/**
 * Evaluates to true, if one join partner is NULL.
 * @param <L> left type
 * @param <R> right type
 */
public class WithEmptySide<L, R>
  implements FlatJoinFunction<L, R, Either<L, R>> {


  @Override
  public void join(L left, R right, Collector<Either<L, R>> collector) throws
    Exception {

    if (right == null) {
      collector.collect(Either.<L, R>Left(left));
    } else if (left == null) {
      collector.collect(Either.<L, R>Right(right));
    }
  }
}
