package org.gradoop.model.impl.algorithms.fsm.common.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.GSpanTransaction;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PostPruning
  implements FlatMapFunction<CompressedDFSCode, CompressedDFSCode> {

  private final FSMConfig fsmConfig;

  public PostPruning(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void flatMap(CompressedDFSCode compressedDFSCode,
    Collector<CompressedDFSCode> collector) throws Exception {

    DFSCode reportedCode = compressedDFSCode.getDfsCode();
    List<DFSStep> steps = reportedCode.getSteps();
    GSpanTransaction transaction = initTransaction(steps);

    for (int edgeCount = 2; edgeCount <= steps.size(); edgeCount++) {
      GSpan.growEmbeddings(transaction, fsmConfig);
    }

    DFSCode minDfsCode = GSpan.findMinimumDfsCode(
      transaction.getSiblingGroups().iterator().next(), fsmConfig);

    if(reportedCode.equals(minDfsCode)) {
      collector.collect(compressedDFSCode);
    }
  }

  private GSpanTransaction initTransaction(List<DFSStep> steps) {
    DFSStep firstStep = steps.get(0);

    CompressedDFSCode startCode = new CompressedDFSCode(new DFSCode(firstStep));

    Collection<DFSEmbedding> embeddings = Lists.newArrayList();

    Map<Integer, AdjacencyList> adjacencyLists = Maps.newHashMap();

    int edgeId = 0;
    for(DFSStep step : steps) {
      // first step or same direction and labels as first step
      if(edgeId == 0 || samePattern(firstStep, step)){
        addEmbedding(embeddings, edgeId, step, false);
        // inverse direction but same labels as first step
      } else if (inversePattern(firstStep, step)) {
        addEmbedding(embeddings, edgeId, step, true);
      }

      int fromId = step.getFromTime();
      int fromLabel = step.getFromLabel();
      boolean outgoing = step.isOutgoing();
      int edgeLabel = step.getEdgeLabel();
      int toId = step.getToTime();
      int toLabel = step.getToLabel();

      addEntry(adjacencyLists,
        fromId, fromLabel, outgoing, edgeId, edgeLabel, toId, toLabel);
      addEntry(adjacencyLists,
        toId, toLabel, !outgoing, edgeId, edgeLabel, fromId, fromLabel);

      edgeId++;
    }

    return new GSpanTransaction(adjacencyLists,
      initCodeEmbeddings(startCode, embeddings), initCodeSiblings(startCode));
  }

  private void addEmbedding(Collection<DFSEmbedding> embeddings,
    int edgeId, DFSStep step, boolean inverse) {

    List<Integer> vertexTimes;

    if (step.isLoop()) {
      vertexTimes = Lists.newArrayList(step.getFromTime());
    } else if(!inverse) {
      vertexTimes = Lists.newArrayList(step.getFromTime(), step.getToTime());
    } else {
      vertexTimes = Lists.newArrayList(step.getToTime(), step.getFromTime());
    }

    List<Integer> edgeTimes = Lists.newArrayList(edgeId);
    embeddings.add(new DFSEmbedding(vertexTimes, edgeTimes));
  }

  private boolean inversePattern(DFSStep firstStep, DFSStep secondStep) {
    return ! secondStep.isOutgoing().equals(firstStep.isOutgoing()) &&
      secondStep.isLoop().equals(firstStep.isLoop()) &&
      secondStep.getFromLabel().equals(firstStep.getToLabel()) &&
      secondStep.getToLabel().equals(firstStep.getFromLabel());
  }

  private boolean samePattern(DFSStep firstStep, DFSStep secondStep) {
    return secondStep.isOutgoing().equals(firstStep.isOutgoing()) &&
      secondStep.isLoop().equals(firstStep.isLoop()) &&
      secondStep.getFromLabel().equals(firstStep.getFromLabel()) &&
    secondStep.getToLabel().equals(firstStep.getToLabel());
  }

  private void addEntry(Map<Integer, AdjacencyList> adjacencyLists,
    int fromId, int fromLabel,
    boolean outgoing, int edgeId, int edgeLabel,
    int toId, int toLabel) {

    AdjacencyList adjacencyList = adjacencyLists.get(fromId);

    AdjacencyListEntry entry =
      new AdjacencyListEntry(outgoing, edgeId, edgeLabel, toId, toLabel);

    if(adjacencyList == null) {
      adjacencyLists.put(fromId, new AdjacencyList(fromLabel, entry));
    } else {
      adjacencyList.getEntries().add(entry);
    }
  }

  private Collection<Collection<CompressedDFSCode>> initCodeSiblings(
    CompressedDFSCode startCode) {
    Collection<CompressedDFSCode> siblings = Lists
      .newArrayList(startCode);
    Collection<Collection<CompressedDFSCode>> codeSiblings =
      Lists.newArrayListWithExpectedSize(1);
    codeSiblings.add(siblings);
    return codeSiblings;
  }

  private Map<CompressedDFSCode, Collection<DFSEmbedding>> initCodeEmbeddings(
    CompressedDFSCode startCode, Collection<DFSEmbedding> embeddings) {
    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings =
      Maps.newHashMap();
    codeEmbeddings.put(startCode, embeddings);
    return codeEmbeddings;
  }
}
