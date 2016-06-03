package org.gradoop.model.impl.algorithms.fsm;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.datagen.fsmtransactions.PredictableFSMTransactionGenerator;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.api.TransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common
  .GradoopTransactionalFSMEncoder;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedSubgraph;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.WithCount;
import org.gradoop.model.impl.algorithms.fsm.filterrefine
  .FilterRefineGSpanMiner;
import org.gradoop.model.impl.algorithms.fsm.iterative.IterativeGSpanMiner;
import org.gradoop.model.impl.algorithms.fsm.pre.tuples.EdgeTriple;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

public class TransactionalFSMinerTest   extends GradoopFlinkTestBase {

  @Test
  public void testMinersSeparately() throws Exception {
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collection =
      new PredictableFSMTransactionGenerator<>(getConfig(), 100).execute();

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(1.0f);

    GradoopTransactionalFSMEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GradoopTransactionalFSMEncoder<>();

    DataSet<EdgeTriple> edges = encoder.encode(collection, fsmConfig);

    for (TransactionalFSMiner miner : getTransactionalFSMiners()) {
      miner.setExecutionEnvironment(
        collection.getConfig().getExecutionEnvironment());
      DataSet<WithCount<CompressedSubgraph>> frequentSubgraphs =
        miner.mine(edges, encoder.getMinSupport(), fsmConfig);

      Assert.assertEquals(702, frequentSubgraphs.count());
    }
  }

  private Collection<TransactionalFSMiner> getTransactionalFSMiners() {
    Collection<TransactionalFSMiner> miners = Lists.newArrayList();

    miners.add(new IterativeGSpanMiner());
    miners.add(new FilterRefineGSpanMiner());
    return miners;
  }

  @Test
  public void testMinersVersus() throws Exception {
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collection =
      new PredictableFSMTransactionGenerator<>(getConfig(), 10).execute();

    FSMConfig fsmConfig = FSMConfig.forDirectedMultigraph(0.4f);

    GradoopTransactionalFSMEncoder<GraphHeadPojo, VertexPojo, EdgePojo>
      encoder = new GradoopTransactionalFSMEncoder<>();

    DataSet<EdgeTriple> edges = encoder.encode(collection, fsmConfig);

    TransactionalFSMiner iMiner = new IterativeGSpanMiner();
    iMiner.setExecutionEnvironment(
      collection.getConfig().getExecutionEnvironment());
    DataSet<WithCount<CompressedSubgraph>> iResult =
      iMiner.mine(edges, encoder.getMinSupport(), fsmConfig);

    TransactionalFSMiner fsMiner = new FilterRefineGSpanMiner();
    DataSet<WithCount<CompressedSubgraph>> frResult =
      fsMiner.mine(edges, encoder.getMinSupport(), fsmConfig);

    collectAndAssertTrue(Equals
      .cross(Count.count(iResult),
        Count.count(iResult.join(frResult).where(0).equalTo(0)))
    );
  }
}