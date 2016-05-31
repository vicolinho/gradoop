package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.DfsCodeTranslator;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;

/**
 * Created by peet on 20.05.16.
 */
public class EqualSupport
  extends RichFlatJoinFunction<CompressedDfsCode, CompressedDfsCode, Boolean> {

  private DfsCodeTranslator translator;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    translator = new DfsCodeTranslator(getRuntimeContext());
  }


  @Override
  public void join(
    CompressedDfsCode left, CompressedDfsCode right, Collector<Boolean> collector
  ) throws Exception {

    String out;

    if(right == null) {
      out = translator.translate(
        left.getDfsCode()) + " " + left.getSupport() + "/-";
    } else if(left == null) {
      out = translator.translate(right.getDfsCode()) + " -/" + right.getSupport();
    } else {
      out = translator.translate(left.getDfsCode()) +
        " "  + left.getSupport() + "/" + right.getSupport();
    }

    boolean equal = left != null && right != null &&
      left.getSupport().equals(right.getSupport());

    if(! equal) {
      System.out.println(out + " " +
        (left == null ? right.getDfsCode() : left.getDfsCode()));
    }

    collector.collect(equal);
  }
}
