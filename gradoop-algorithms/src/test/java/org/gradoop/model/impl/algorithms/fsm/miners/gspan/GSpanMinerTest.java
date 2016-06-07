package org.gradoop.model.impl.algorithms.fsm.miners.gspan;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.datagen.transactions.predictable.PredictableTransactionsGenerator;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.algorithms.fsm.config.FsmConfig;
import org.gradoop.model.impl.algorithms.fsm.miners.TransactionalFsMiner;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.encoders.GraphCollectionTFsmEncoder;
import org.gradoop.model.impl.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine.GSpanFilterRefine;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration.GSpanBulkIteration;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

public class GSpanMinerTest extends GradoopFlinkTestBase {

  @Test
  public void testMinersSeparately() throws Exception {
    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> transactions =
      new PredictableTransactionsGenerator<>(100, 1, true, getConfig())
        .execute();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collection =
      GraphCollection
        .fromTransactions(transactions.getTransactions(), getConfig());

    FsmConfig fsmConfig = FsmConfig.forDirectedMultigraph(1.0f);

    GraphCollectionTFsmEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GraphCollectionTFsmEncoder<>();

    DataSet<EdgeTriple> edges = encoder.encode(collection, fsmConfig);

    for (TransactionalFsMiner miner : getTransactionalFSMiners()) {
      miner.setExecutionEnvironment(
        collection.getConfig().getExecutionEnvironment());
      DataSet<WithCount<CompressedSubgraph>> frequentSubgraphs =
        miner.mine(edges, encoder.getMinFrequency(), fsmConfig);

      Assert.assertEquals(702, frequentSubgraphs.count());
    }
  }

  private Collection<TransactionalFsMiner> getTransactionalFSMiners() {
    Collection<TransactionalFsMiner> miners = Lists.newArrayList();

    miners.add(new GSpanBulkIteration());
    miners.add(new GSpanFilterRefine());
    return miners;
  }

  @Test
  public void testMinersVersus() throws Exception {
    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> transactions =
      new PredictableTransactionsGenerator<>(30, 1, true, getConfig())
        .execute();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collection =
      GraphCollection
        .fromTransactions(transactions.getTransactions(), getConfig());

    FsmConfig fsmConfig = FsmConfig.forDirectedMultigraph(0.4f);

    GraphCollectionTFsmEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GraphCollectionTFsmEncoder<>();

    DataSet<EdgeTriple> edges = encoder.encode(collection, fsmConfig);

    TransactionalFsMiner iMiner = new GSpanBulkIteration();
    iMiner.setExecutionEnvironment(
      collection.getConfig().getExecutionEnvironment());
    DataSet<WithCount<CompressedSubgraph>> iResult =
      iMiner.mine(edges, encoder.getMinFrequency(), fsmConfig);

    TransactionalFsMiner fsMiner = new GSpanFilterRefine();
    DataSet<WithCount<CompressedSubgraph>> frResult =
      fsMiner.mine(edges, encoder.getMinFrequency(), fsmConfig);

    collectAndAssertTrue(Equals
      .cross(Count.count(iResult),
        Count.count(iResult.join(frResult).where(0).equalTo(0)))
    );
  }
}