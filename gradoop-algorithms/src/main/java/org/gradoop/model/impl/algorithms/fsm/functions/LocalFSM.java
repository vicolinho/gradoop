package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.pojos.AdjacencyLists;
import org.gradoop.model.impl.algorithms.fsm.pojos.SearchSpacePartition;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDFSCode;

import java.util.ArrayList;

public class LocalFSM implements
  MapFunction<SearchSpacePartition, SearchSpacePartition> {
  public static final String DS_NAME = "minSupport";

  @Override
  public SearchSpacePartition map(
    SearchSpacePartition searchSpacePartition) throws Exception {

    return null;
  }
}
