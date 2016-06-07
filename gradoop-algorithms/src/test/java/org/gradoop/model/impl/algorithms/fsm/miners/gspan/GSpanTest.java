package org.gradoop.model.impl.algorithms.fsm.miners.gspan;

import com.google.common.collect.Lists;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.encoders.TransactionalFSMEncoder;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.GSpan;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.DfsStep;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.GSpanTransaction;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.encoders.GraphCollectionTFSMEncoder;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.*;

public class GSpanTest extends GradoopFlinkTestBase {

  @Test
  public void testMinDfsCodeCalculation() {

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.7f);

    //       -a->
    //  (0:A)    (1:A)
    //       -a->

    DfsStep firstStep = new DfsStep(0, 0, true, 0, 1, 0);
    DfsStep backwardStep = new DfsStep(1, 0, false, 0, 0, 0);
    DfsStep branchStep = new DfsStep(0, 0, true, 0, 1, 0);

    DfsCode minCode = new DfsCode(Lists.newArrayList(firstStep, backwardStep));
    DfsCode wrongCode = new DfsCode(Lists.newArrayList(firstStep, branchStep));

    assertTrue(
      GSpan.isMinimumDfsCode(minCode, fsmConfig));
    assertFalse(
      GSpan.isMinimumDfsCode(wrongCode, fsmConfig));
  }

  @Test
  public void testDiamondMining() throws Exception {

    String asciiGraphs = "" +
      "g1[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +
      "g2[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +
      "g3[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +

      "s1[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)-[:a]->(v4:A)]" +

      "s2[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);(v1:A)-[:a]->(v3:A)             ]" +
      "s3[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A);             (v3:A)-[:a]->(v4:A)]" +
      "s4[(v1:A)-[:a]->(v2:A)-[:a]->(v4:A)                                 ]" +
      "s5[(v1:A)-[:a]->(v2:A)             ;(v1:A)-[:a]->(v3:A)             ]" +
      "s6[             (v2:A)-[:a]->(v4:A);             (v3:A)-[:a]->(v4:A)]" +
      "s7[(v1:A)-[:a]->(v2:A)                                              ]";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> searchSpace =
      loader.getGraphCollectionByVariables("g1");

    TransactionalFSMEncoder
      <GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>> encoder =
      new GraphCollectionTFSMEncoder<>();

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.7f);


    Collection<EdgeTriple> edges =
      encoder.encode(searchSpace, fsmConfig).collect();

    // create GSpanTransaction
    GSpanTransaction transaction = GSpan.createTransaction(edges);

    assertEquals(1, transaction.getCodeEmbeddings().size());

    assertEquals(4,
      transaction.getCodeEmbeddings().values().iterator().next().size());

    // N=1
    Collection<DfsCode> singleEdgeCodes =
      transaction.getCodeEmbeddings().keySet();

    assertEquals(singleEdgeCodes.size(), 1);

    DfsCode singleEdgeCode =
      singleEdgeCodes.iterator().next();

    assertEquals(singleEdgeCode, new DfsCode(new DfsStep(0, 0, true, 0, 1, 0)));

    // N=2
    assertEquals(0, singleEdgeCode.getMinVertexLabel());

    GSpan.growEmbeddings(transaction, singleEdgeCodes,fsmConfig);

    Collection<DfsCode> twoEdgeCodes =
      transaction.getCodeEmbeddings().keySet();

    assertEquals(4, twoEdgeCodes.size());

    // post pruning
    Iterator<DfsCode> iterator = twoEdgeCodes.iterator();

    while (iterator.hasNext()) {
      DfsCode subgraph = iterator.next();

      if (!GSpan.isMinimumDfsCode(subgraph, fsmConfig)) {
        iterator.remove();
      }
    }

    assertEquals(3, twoEdgeCodes.size());

    // N=3

    DfsCode minSubgraph =
      GSpan.minimumDfsCode(twoEdgeCodes, fsmConfig);

    GSpan.growEmbeddings(
      transaction, Lists.newArrayList(minSubgraph), fsmConfig);

    Collection<DfsCode> threeEdgeCodes =
      transaction.getCodeEmbeddings().keySet();

    assertEquals(2, threeEdgeCodes.size());

    // post pruning
    iterator = threeEdgeCodes.iterator();

    while (iterator.hasNext()) {
      DfsCode subgraph = iterator.next();

      if (!GSpan.isMinimumDfsCode(subgraph, fsmConfig)) {
        iterator.remove();
      }
    }

    assertEquals(2, threeEdgeCodes.size());

    // N=4

    minSubgraph = GSpan.minimumDfsCode(threeEdgeCodes, fsmConfig);

    GSpan.growEmbeddings(
      transaction, Lists.newArrayList(minSubgraph), fsmConfig);

    Collection<DfsCode> fourEdgeCodes =
      transaction.getCodeEmbeddings().keySet();

    assertEquals(1, fourEdgeCodes.size());
  }
}