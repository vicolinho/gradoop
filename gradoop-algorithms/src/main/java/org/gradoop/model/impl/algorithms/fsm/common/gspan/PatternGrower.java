package org.gradoop.model.impl.algorithms.fsm.common.gspan;

import com.google.common.collect.Lists;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.gradoop.model.impl.algorithms.fsm.common.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyList;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.AdjacencyListEntry;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSEmbedding;
import org.gradoop.model.impl.algorithms.fsm.common.pojos.DFSStep;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.common.tuples.Transaction;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PatternGrower implements Serializable {

  private final DfsCodeSiblingComparator siblingComparator;

  public PatternGrower(FSMConfig fsmConfig) {
    boolean directed = fsmConfig.isDirected();
    this.siblingComparator = new DfsCodeSiblingComparator(directed);
  }

  public void growEmbeddings(final Transaction transaction) {

    Map<CompressedDFSCode, Collection<DFSEmbedding>> childCodeEmbeddings = null;
    Collection<Collection<CompressedDFSCode>> childSiblingGroups = null;

    // for each leaf on leftmost branch in DFS code tree
    for (Collection<CompressedDFSCode> parentSiblings : transaction.getSiblingGroups()) {

      if (!parentSiblings.isEmpty()) {
        Collection<CompressedDFSCode> childSiblings = null;

        DFSCode parentCode = findMinimumDfsCode(parentSiblings);

        List<Integer> rightmostPath = parentCode.getRightMostPathVertexTimes();

        // for each embedding
        for (DFSEmbedding parentEmbedding : transaction.getCodeEmbeddings()
          .get(new CompressedDFSCode(parentCode))) {

          // first iterated vertex is rightmost
          Boolean rightMostVertex = true;
          List<Integer> vertexTimes = parentEmbedding.getVertexTimes();

          // for each time on rightmost path
          for (Integer fromVertexTime : rightmostPath) {

            // query fromVertex data
            AdjacencyList adjacencyList = transaction.getAdjacencyLists()
              .get(vertexTimes.get(fromVertexTime));
            Integer fromVertexLabel = adjacencyList.getFromVertexLabel();

            // for each incident edge
            for (AdjacencyListEntry entry : adjacencyList.getEntries()) {

              // if valid extension for branch
              if (entry.getMinEdgePatternId() >=
                parentEmbedding.getMinEdgePatternId()) {

                Integer edgeId = entry.getEdgeId();

                // if edge not already contained
                if (!parentEmbedding.getEdgeTimes()
                  .contains(edgeId)) {
                  // query toVertexData
                  Integer toVertexId = entry.getToVertexId();
                  Integer toVertexTime = vertexTimes.indexOf(toVertexId);
                  boolean forward = toVertexTime < 0;

                  // PRUNING : grow only forward
                  // or backward from rightmost vertex
                  if (forward || rightMostVertex) {

                    DFSEmbedding childEmbedding =
                      DFSEmbedding.deepCopy(parentEmbedding);
                    DFSCode childCode = DFSCode.deepCopy(parentCode);

                    // add new vertex to embedding for forward steps
                    if (forward) {
                      childEmbedding.getVertexTimes().add(toVertexId);
                      toVertexTime = childEmbedding.getVertexTimes().size() - 1;
                    }

                    childCode.getSteps().add(new DFSStep(
                      fromVertexTime,
                      fromVertexLabel,
                      entry.isOutgoing(),
                      entry.getEdgeLabel(),
                      toVertexTime,
                      entry.getToVertexLabel()
                    ));

                    childEmbedding.getEdgeTimes().add(edgeId);

                    CompressedDFSCode compressedChildCode =
                      new CompressedDFSCode(childCode);

                    childSiblings = addSibling(
                      childSiblings, compressedChildCode);

                    childCodeEmbeddings = addCodeEmbedding(
                      childCodeEmbeddings, compressedChildCode, childEmbedding);
                  }
                }
              }
            }
            rightMostVertex = false;
          }
        }
        childSiblingGroups = addSiblings(childSiblingGroups, childSiblings);
      }
    }

    transaction.setCodeEmbeddings(childCodeEmbeddings);
    transaction.setCodeSiblings(childSiblingGroups);

  }

  private DFSCode findMinimumDfsCode(Collection<CompressedDFSCode> dfsCodes) {

    Iterator<CompressedDFSCode> iterator = dfsCodes.iterator();

    DFSCode minCode = iterator.next().getDfsCode();

    while (iterator.hasNext()) {
      DFSCode nextCode = iterator.next().getDfsCode();

      if(siblingComparator.compare(nextCode, minCode) < 0) {
        minCode = nextCode;
      }
    }

    return minCode;
  }

  private Collection<CompressedDFSCode> addSibling(
    Collection<CompressedDFSCode> siblings, CompressedDFSCode code) {

    if (siblings == null) {
      siblings = Lists.newArrayList(code);
    } else {
      siblings.add(code);
    }

    return siblings;
  }

  private Collection<Collection<CompressedDFSCode>> addSiblings(
    Collection<Collection<CompressedDFSCode>> siblingGroups,
    Collection<CompressedDFSCode> siblings) {

    if(siblings != null) {
      if (siblingGroups == null) {
        siblingGroups = Lists.newArrayList();
      }

      siblingGroups.add(siblings);
    }

    return siblingGroups;
  }

  private Map<CompressedDFSCode, Collection<DFSEmbedding>> addCodeEmbedding(
    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings,
    CompressedDFSCode code, DFSEmbedding embedding) {

    Collection<DFSEmbedding> embeddings;

    if(codeEmbeddings == null) {
      codeEmbeddings = Maps.newHashMap();
      codeEmbeddings.put(code, Lists.newArrayList(embedding));
    } else {
      embeddings = codeEmbeddings.get(code);

      if(embeddings == null) {
        codeEmbeddings.put(code, Lists.newArrayList(embedding));
      } else {
        embeddings.add(embedding);
      }
    }
    return codeEmbeddings;
  }


  public static void prune(final Transaction transaction,
    Collection<CompressedDFSCode> frequentDfsCodes) {

    Iterator<Collection<CompressedDFSCode>> groupIterator = transaction
      .getSiblingGroups().iterator();

    while (groupIterator.hasNext()) {
      Collection<CompressedDFSCode> group = groupIterator.next();
      Iterator<CompressedDFSCode> codeIterator = group.iterator();

      while (codeIterator.hasNext()) {
        CompressedDFSCode code = codeIterator.next();

        if(! frequentDfsCodes.contains(code)) {
          // drop embeddings
          transaction.getCodeEmbeddings().remove(code);
          // remove from siblings
          codeIterator.remove();
        }
      }

      if(group.isEmpty()) {
        // drop empty sibling group
        groupIterator.remove();
      }
    }
  }
}
