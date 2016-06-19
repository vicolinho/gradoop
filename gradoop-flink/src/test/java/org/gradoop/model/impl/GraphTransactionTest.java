package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.functions.utils.First;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

public class GraphTransactionTest extends GradoopFlinkTestBase {

  @Test
  public void testTransformation() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> originalCollection =
      loader
        .getDatabase()
        .getCollection();

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo>
      transactions = originalCollection.toTransactions();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> restoredCollection =
      GraphCollection.fromTransactions(transactions);

    collectAndAssertTrue(
      originalCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphData(restoredCollection));
  }

  @Test
  public void testTransformationWithBroadcast() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> originalCollection =
      loader
        .getDatabase()
        .getCollection();

    List<GradoopId> graphIds = Lists.newArrayList();
    List<GradoopId> includeGraphIds = Lists.newArrayList();

    String[] graphVariables = new String[]{"g0"};
    String[] includeVariables = new String[]{"g0"};

    for(String graphVariable : graphVariables) {
      graphIds.add(loader.getGraphHeadByVariable(graphVariable).getId());
    }
    for(String graphVariable : includeVariables) {
      includeGraphIds.add(loader.getGraphHeadByVariable(graphVariable).getId());
    }

    GradoopIdSet graphIdSet = GradoopIdSet.fromExisting(graphIds);
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> filteredCollection =
      originalCollection.getGraphs(graphIdSet);

    DataSet<GradoopId> excludeBroadcast = originalCollection.getConfig().
      getExecutionEnvironment().fromCollection(includeGraphIds);
    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo>
      transactions = filteredCollection.toTransactions(excludeBroadcast,false);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> restoredCollection =
      GraphCollection.fromTransactions(transactions);

    collectAndAssertTrue(
      filteredCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      filteredCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      filteredCollection.equalsByGraphData(restoredCollection));
  }

  @Test
  public void testTransformationWithBroadcastExclusion() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
            loader = getSocialNetworkLoader();
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> originalCollection =
      loader
        .getDatabase()
        .getCollection();

    List<GradoopId> graphIds = Lists.newArrayList();
    List<GradoopId> excludeGraphIds = Lists.newArrayList();

    String[] graphVariables = new String[]{"g0","g1","g2"};
    String[] excludeVariables = new String[]{"g3"};

    for(String graphVariable : graphVariables) {
      graphIds.add(loader.getGraphHeadByVariable(graphVariable).getId());
    }
    for(String graphVariable : excludeVariables) {
      excludeGraphIds.add(loader.getGraphHeadByVariable(graphVariable).getId());
    }

    GradoopIdSet graphIdSet = GradoopIdSet.fromExisting(graphIds);
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> filteredCollection =
      originalCollection.getGraphs(graphIdSet);

    DataSet<GradoopId> excludeBroadcast = originalCollection.getConfig().
      getExecutionEnvironment().fromCollection(excludeGraphIds);
    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo>
      transactions = filteredCollection.toTransactions(excludeBroadcast,true);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> restoredCollection =
      GraphCollection.fromTransactions(transactions);

    collectAndAssertTrue(
      filteredCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      filteredCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      filteredCollection.equalsByGraphData(restoredCollection));
  }

  @Test
  public void testTransformationWithCustomReducer() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> originalCollection =
      loader
        .getDatabase()
        .getCollection();

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> transactions =
      originalCollection.toTransactions();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> restoredCollection =
      GraphCollection.fromTransactions(transactions,
        new First<VertexPojo>(), new First<EdgePojo>());

    collectAndAssertTrue(
      originalCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphData(restoredCollection));
  }

}