package org.gradoop.model.impl.algorithms.fsm.common.gspan;


import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Collections;
import java.util.List;

public class Coverage {


  private final int hashCode;

  public Coverage(int hashCode) {
    this.hashCode = hashCode;
  }

  public static Coverage fromIdList(List<Integer> ids) {
    HashCodeBuilder builder = new HashCodeBuilder();

    Collections.sort(ids);

    for(Integer id : ids) {
      builder.append(id);
    }

    return new Coverage(builder.hashCode());
  }

  @Override
  public int hashCode() {
    return this.hashCode;
  }

  @Override
  public boolean equals(Object o) {
    return this.hashCode() == o.hashCode();
  }
}
