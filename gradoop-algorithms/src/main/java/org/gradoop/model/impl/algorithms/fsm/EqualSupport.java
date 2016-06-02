package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.DfsCodeTranslator;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.ObjectWithCount;

/**
 * Created by peet on 20.05.16.
 */
public class EqualSupport
  extends RichFlatJoinFunction<ObjectWithCount<CompressedSubgraph>, ObjectWithCount<CompressedSubgraph>, Boolean> {

  private DfsCodeTranslator translator;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    translator = new DfsCodeTranslator(getRuntimeContext());
  }


  @Override
  public void join(
    ObjectWithCount<CompressedSubgraph> left, ObjectWithCount<CompressedSubgraph> right, Collector<Boolean> collector
  ) throws Exception {

    String out;

    if(right == null) {
      out = translator.translate(
        left.getObject().getDfsCode()) + " " + left.getSupport() + "/-";
    } else if(left == null) {
      out = translator.translate(right.getObject().getDfsCode()) + " -/" + right.getSupport();
    } else {
      out = translator.translate(left.getObject().getDfsCode()) +
        " "  + left.getSupport() + "/" + right.getSupport();
    }

    boolean equal = left != null && right != null &&
      left.getSupport().equals(right.getSupport());

    if(! equal) {
      System.out.println(out + " " +
        (left == null ? right.getObject().getDfsCode() : left.getObject().getDfsCode()));
    }

    collector.collect(equal);
  }
}
