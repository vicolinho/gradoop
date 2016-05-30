package org.gradoop.model.impl.algorithms.fsm.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class FlipDictionary implements
  MapFunction<List<String>, Map<String, Integer>> {

  @Override
  public Map<String, Integer> map(List<String> intStringDictionary)
    throws Exception {

    int labelCount = intStringDictionary.size();

    Map<String, Integer> stringIntDictionary = Maps
      .newHashMapWithExpectedSize(labelCount);

    for(int i = 0; i < labelCount; i++) {
      stringIntDictionary.put(intStringDictionary.get(i), i);
    }

    return stringIntDictionary;
  }
}
