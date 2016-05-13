package org.gradoop.model.impl.algorithms.fsm.filterrefine.pojos;


import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public abstract class StepTriple {

  protected final int sourceValue;
  protected final int edgeValue;
  protected final int targetValue;

  public StepTriple(int sourceId, int edgeId, int targetId) {
    this.sourceValue = sourceId;
    this.edgeValue = edgeId;
    this.targetValue = targetId;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    builder.append(sourceValue);
    builder.append(edgeValue);
    builder.append(targetValue);

    return builder.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    boolean equals = obj == this;

    if (!equals && obj != null && obj.getClass() == getClass()) {

      StepTriple other = (StepTriple) obj;

      EqualsBuilder builder = new EqualsBuilder();

      builder.append(sourceValue, other.sourceValue);
      builder.append(edgeValue, other.edgeValue);
      builder.append(targetValue, other.targetValue);

      equals = builder.isEquals();
    }

    return equals;
  }

  @Override
  public String toString() {
    return "(" + sourceValue + ")-" + edgeValue + "->(" + targetValue + ")";
  }
}
