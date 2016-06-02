package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples.SubgraphMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class FrequentOrRefinementCandidate
  extends RichGroupReduceFunction
  <SubgraphMessage, SubgraphMessage> {

  private final float threshold;
  /**
   * minimum support
   */
  private Integer minSupport;
  private Map<Integer, Integer> workerGraphCount;

  public FrequentOrRefinementCandidate(FSMConfig fsmConfig) {
    this.threshold = fsmConfig.getThreshold();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.minSupport = getRuntimeContext()
      .<Integer>getBroadcastVariable(
        BroadcastNames.MIN_SUPPORT).get(0);

    this.workerGraphCount = getRuntimeContext()
      .<Map<Integer, Integer>>getBroadcastVariable(
        BroadcastNames.WORKER_GRAPHCOUNT).get(0);
  }

  @Override
  public void reduce(
    Iterable<SubgraphMessage> iterable,
    Collector<SubgraphMessage> collector) throws
    Exception {

    ArrayList<SubgraphMessage> x = Lists.newArrayList(iterable);

    // copy list of all workers
    Collection<Integer> workerIdsWithoutReport = Sets
      .newHashSet(workerGraphCount.keySet());

    // init aggregate variables
    CompressedDfsCode code = null;
    int support = 0;
    boolean atLeastOnceLocallyFrequent = false;

    // for each worker report
    for(Tuple4<CompressedDfsCode, Integer, Integer, Boolean> triple : x) {
      code = triple.f0;

      Integer reportedWorkerId = triple.f2;
      Boolean locallyFrequent = triple.f3;

      support += triple.f1;

      if(!atLeastOnceLocallyFrequent && locallyFrequent) {
        atLeastOnceLocallyFrequent = true;
      }

      workerIdsWithoutReport.remove(reportedWorkerId);
    }


    // CANDIDATE SELECTION

    if(atLeastOnceLocallyFrequent) {

      // support of all workers known
      if(workerIdsWithoutReport.isEmpty()) {
        // if globally frequent
        if(support >= minSupport) {
          // emit complete support message
          collector.collect(new SubgraphMessage(code, support, -1, true));
        }
      } else {
        int estimation = support;

        // add optimistic support estimations
        for(Integer workerId : workerIdsWithoutReport) {
          estimation += (workerGraphCount.get(workerId) * threshold);
        }
        // if likely globally frequent
        if(estimation >= minSupport) {
          // emit incomplete support message
          collector.collect(new SubgraphMessage(code, support, -1, false));

          // add refinement calls to missing workers
          for(Integer workerId : workerIdsWithoutReport) {
            collector.collect(new SubgraphMessage(code, 0, workerId, false));
          }
        }
      }
    }
  }
}
