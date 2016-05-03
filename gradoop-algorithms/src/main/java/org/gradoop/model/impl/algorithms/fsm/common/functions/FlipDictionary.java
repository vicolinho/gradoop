package org.gradoop.model.impl.algorithms.fsm.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;

public class FlipDictionary implements
  MapFunction<ArrayList<String>, HashMap<String, Integer>> {

  @Override
  public HashMap<String, Integer> map(ArrayList<String> intStringDictionary)
    throws Exception {

    int labelCount = intStringDictionary.size();

    HashMap<String, Integer> stringIntDictionary = Maps
      .newHashMapWithExpectedSize(labelCount);

    for(int i = 0; i < labelCount; i++) {
      stringIntDictionary.put(intStringDictionary.get(i), i);
    }

    return stringIntDictionary;
  }
}
