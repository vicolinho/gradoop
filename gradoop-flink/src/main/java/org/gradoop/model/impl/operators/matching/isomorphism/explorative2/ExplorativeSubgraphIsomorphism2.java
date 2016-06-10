package org.gradoop.model.impl.operators.matching.isomorphism.explorative2;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.log4j.Logger;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.utils.Superstep;
import org.gradoop.model.impl.operators.matching.common.PreProcessor;
import org.gradoop.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.model.impl.operators.matching.common.functions.ElementHasCandidate;
import org.gradoop.model.impl.operators.matching.common.query.Traverser;
import org.gradoop.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.ExplorativeSubgraphIsomorphism;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.debug.PrintEmbeddingWithWeldPoint;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.functions.BuildEmbeddingWithTiePoint;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative2.functions.UpdateEdgeMappings;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative.tuples.EmbeddingWithTiePoint;
import org.gradoop.model.impl.operators.matching.isomorphism.explorative2.functions.EdgeHasCandidate;
import org.gradoop.model.impl.operators.matching.simulation.dual.debug.PrintTripleWithDirection;
import org.gradoop.model.impl.operators.matching.simulation.dual.functions.CloneAndReverse;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.TripleWithDirection;

/**
 * Modified version of {@link ExplorativeSubgraphIsomorphism} that duplicates
 * the edge set and filters/joins only on that single data set.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class ExplorativeSubgraphIsomorphism2
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends ExplorativeSubgraphIsomorphism<G, V, E> {

  private static final Logger LOG = Logger.getLogger
    (ExplorativeSubgraphIsomorphism2.class);

  public ExplorativeSubgraphIsomorphism2(String query, boolean attachData) {
    super(query, attachData);
  }

  public ExplorativeSubgraphIsomorphism2(String query, boolean attachData,
    Traverser traverser) {
    super(query, attachData, traverser);
  }

  public ExplorativeSubgraphIsomorphism2(String query, boolean attachData,
    Traverser traverser, JoinOperatorBase.JoinHint edgeStepJoinStrategy,
    JoinOperatorBase.JoinHint vertexStepJoinStrategy) {
    super(query, attachData, traverser, edgeStepJoinStrategy,
      vertexStepJoinStrategy);
  }

  @Override
  protected GraphCollection<G, V, E> executeForPattern(
    LogicalGraph<G, V, E> graph) {
    //--------------------------------------------------------------------------
    // Pre-processing (filter candidates + build initial embeddings)
    //--------------------------------------------------------------------------

    DataSet<IdWithCandidates> vertices = PreProcessor.filterVertices(
      graph, getQuery());

    DataSet<TripleWithDirection> edges = PreProcessor
      .filterEdges(graph, getQuery())
      .flatMap(new CloneAndReverse());

    DataSet<EmbeddingWithTiePoint> initialEmbeddings = vertices
      .filter(new ElementHasCandidate(getTraversalCode().getStep(0).getFrom()))
      .map(new BuildEmbeddingWithTiePoint(getTraversalCode(),
        getQueryHandler().getVertexCount(), getQueryHandler().getEdgeCount()));

    if (LOG.isDebugEnabled()) {
      initialEmbeddings = initialEmbeddings
        .map(new PrintEmbeddingWithWeldPoint())
        .withBroadcastSet(getVertexMapping(), Printer.VERTEX_MAPPING)
        .withBroadcastSet(getEdgeMapping(), Printer.EDGE_MAPPING);
    }

    //--------------------------------------------------------------------------
    // Exploration via Traversal
    //--------------------------------------------------------------------------

    DataSet<EmbeddingWithTiePoint> result = explore(
      vertices, edges, initialEmbeddings);

    //--------------------------------------------------------------------------
    // Post-Processing (build Graph Collection from embeddings)
    //--------------------------------------------------------------------------

    return postProcess(graph, result);
  }

  private DataSet<EmbeddingWithTiePoint> explore(
    DataSet<IdWithCandidates> vertices, DataSet<TripleWithDirection> edges,
    DataSet<EmbeddingWithTiePoint> initialEmbeddings) {
    // ITERATION HEAD
    IterativeDataSet<EmbeddingWithTiePoint> embeddings = initialEmbeddings
      .iterate(getTraversalCode().getSteps().size());

    // ITERATION BODY

    // get current superstep
    DataSet<Integer> superstep = embeddings
      .first(1)
      .map(new Superstep<EmbeddingWithTiePoint>());

    // traverse to outgoing/incoming edges
    DataSet<EmbeddingWithTiePoint> nextWorkSet =
      traverseEdges(edges, superstep, embeddings);

    // traverse to vertices
    nextWorkSet = traverseVertices(vertices, superstep, nextWorkSet);

    // ITERATION FOOTER
    return embeddings.closeWith(nextWorkSet, nextWorkSet);
  }

  /**
   * Traverse to the next edges according to the current traversal code.
   *
   * @param edges       edge candidates
   * @param superstep   current superstep
   * @param embeddings  current embeddings
   * @return grown embeddings
   */
  private DataSet<EmbeddingWithTiePoint> traverseEdges(
    DataSet<TripleWithDirection> edges, DataSet<Integer> superstep,
    IterativeDataSet<EmbeddingWithTiePoint> embeddings) {
    DataSet<TripleWithDirection> edgeSteps = edges
      .filter(new EdgeHasCandidate(getTraversalCode()))
      .withBroadcastSet(superstep, BC_SUPERSTEP);

    if (LOG.isDebugEnabled()) {
      edgeSteps = edgeSteps
        .map(new PrintTripleWithDirection(true, "post-filter-map-edge"))
        .withBroadcastSet(getVertexMapping(), Printer.VERTEX_MAPPING)
        .withBroadcastSet(getEdgeMapping(), Printer.EDGE_MAPPING);
    }

    DataSet<EmbeddingWithTiePoint> nextWorkSet = embeddings
      .join(edgeSteps, getEdgeStepJoinStrategy())
      .where(1).equalTo(1) // tiePointId == tiePointId
      .with(new UpdateEdgeMappings(getTraversalCode()));

    if (LOG.isDebugEnabled()) {
      nextWorkSet = nextWorkSet
        .map(new PrintEmbeddingWithWeldPoint(true, "post-edge-update"))
        .withBroadcastSet(getVertexMapping(), Printer.VERTEX_MAPPING)
        .withBroadcastSet(getEdgeMapping(), Printer.EDGE_MAPPING);
    }
    return nextWorkSet;
  }
}
