package org.gradoop.model.impl.operators.matching.isomorphism.explorative.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.utils.Constants;

/**
 * Filters edge triples if their candidates contain a given candidate. This
 * method can only be applied during iteration as the candidate depends on the
 * current super step.
 *
 * Read fields:
 *
 * f3: edge candidates
 */
@FunctionAnnotation.ReadFields("f3")
public class EdgeHasCandidate
  extends RichFilterFunction<TripleWithCandidates> {

  /**
   * Traversal code
   */
  private final TraversalCode traversalCode;

  /**
   * Candidate to test on
   */
  private long candidate;

  /**
   * Constructor
   *
   * @param traversalCode traversal code to determine candidate
   */
  public EdgeHasCandidate(TraversalCode traversalCode) {
    this.traversalCode = traversalCode;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    int step = (int) getRuntimeContext()
      .getBroadcastVariable(Constants.BC_SUPERSTEP).get(0);
    candidate = traversalCode.getStep(step - 1).getVia();
  }

  @Override
  public boolean filter(TripleWithCandidates t) throws Exception {
    return t.getCandidates().contains(candidate);
  }
}