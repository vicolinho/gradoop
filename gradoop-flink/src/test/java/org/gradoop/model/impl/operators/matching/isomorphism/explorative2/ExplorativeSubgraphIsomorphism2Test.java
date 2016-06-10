package org.gradoop.model.impl.operators.matching.isomorphism.explorative2;

import org.gradoop.model.impl.operators.matching.PatternMatching;
import org.gradoop.model.impl.operators.matching.isomorphism.SubgraphIsomorphismTest;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

/**
 * Created by s1ck on 05.06.16.
 */
public class ExplorativeSubgraphIsomorphism2Test
  extends SubgraphIsomorphismTest {

  public ExplorativeSubgraphIsomorphism2Test(String testName, String dataGraph,
    String queryGraph, String[] expectedGraphVariables,
    String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables,
      expectedCollection);
  }

  @Override
  public PatternMatching<GraphHeadPojo, VertexPojo, EdgePojo> getImplementation(
    String queryGraph, boolean attachData) {
    return new ExplorativeSubgraphIsomorphism2<>(queryGraph, attachData);
  }
}
