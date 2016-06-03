package org.gradoop.model.impl.algorithms.fsm.filterrefine.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples.FilterResult;
import org.gradoop.model.impl.algorithms.fsm.filterrefine.tuples
  .RefinementMessage;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Candidate selection for refinement phase
 *
 * FilterResultMessage,.. => RefinementCall,..
 * where FilterResultMessage.subgraph == RefinementCall.subgraph
 */
public class FrequentOrRefinementCandidate
  extends RichGroupReduceFunction
  <FilterResult, RefinementMessage> {

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
    Iterable<FilterResult> messages,
    Collector<RefinementMessage> collector) throws
    Exception {

    // copy list of all workers
    Collection<Integer> workerIdsWithoutReport =
      Lists.newLinkedList(workerGraphCount.keySet());


    // evaluate first message
    Iterator<FilterResult> iterator = messages.iterator();
    FilterResult message = iterator.next();

    CompressedSubgraph subgraph = message.getSubgraph();
    workerIdsWithoutReport.remove(message.getWorkerId());

    int support = message.getSupport();

    boolean atLeastOnceLocallyFrequent = message.isLocallyFrequent();

    // for each worker report
    while (iterator.hasNext()) {
      message = iterator.next();

      support += message.getSupport();

      if(!atLeastOnceLocallyFrequent) {
        atLeastOnceLocallyFrequent = message.isLocallyFrequent();
      }

      workerIdsWithoutReport.remove(message.getWorkerId());
    }


    // CANDIDATE SELECTION

    if(atLeastOnceLocallyFrequent) {
      // support of all workers known
      if(workerIdsWithoutReport.isEmpty()) {
        // if globally frequent
        if(support >= minSupport) {
          // emit complete support message
          collector.collect(new RefinementMessage(subgraph, support,
            RefinementMessage.GLOBALLY_FREQUENT));
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
          collector.collect(new RefinementMessage(subgraph, support,
            RefinementMessage.PARTIAL_RESULT));

          // add refinement calls to missing workers
          for(Integer workerId : workerIdsWithoutReport) {
            collector.collect(new RefinementMessage(subgraph, workerId,
              RefinementMessage.REFINEMENT_CALL));
          }
        }
      }
    }
  }
}
