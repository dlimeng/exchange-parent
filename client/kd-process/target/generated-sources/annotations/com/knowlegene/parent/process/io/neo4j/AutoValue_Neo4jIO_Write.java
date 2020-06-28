

package com.knowlegene.parent.process.io.neo4j;

import javax.annotation.Generated;
import javax.annotation.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_Neo4jIO_Write<T> extends Neo4jIO.Write<T> {

  private final String statement;
  private final long batchSize;
  private final String optionsType;
  private final String label;
  private final Neo4jIO.DriverConfiguration driverConfiguration;

  private AutoValue_Neo4jIO_Write(
      @Nullable String statement,
      long batchSize,
      @Nullable String optionsType,
      @Nullable String label,
      @Nullable Neo4jIO.DriverConfiguration driverConfiguration) {
    this.statement = statement;
    this.batchSize = batchSize;
    this.optionsType = optionsType;
    this.label = label;
    this.driverConfiguration = driverConfiguration;
  }

  @Nullable
  @Override
  String getStatement() {
    return statement;
  }

  @Override
  long getBatchSize() {
    return batchSize;
  }

  @Nullable
  @Override
  String getOptionsType() {
    return optionsType;
  }

  @Nullable
  @Override
  String getLabel() {
    return label;
  }

  @Nullable
  @Override
  Neo4jIO.DriverConfiguration getDriverConfiguration() {
    return driverConfiguration;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Neo4jIO.Write) {
      Neo4jIO.Write<?> that = (Neo4jIO.Write<?>) o;
      return ((this.statement == null) ? (that.getStatement() == null) : this.statement.equals(that.getStatement()))
           && (this.batchSize == that.getBatchSize())
           && ((this.optionsType == null) ? (that.getOptionsType() == null) : this.optionsType.equals(that.getOptionsType()))
           && ((this.label == null) ? (that.getLabel() == null) : this.label.equals(that.getLabel()))
           && ((this.driverConfiguration == null) ? (that.getDriverConfiguration() == null) : this.driverConfiguration.equals(that.getDriverConfiguration()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (statement == null) ? 0 : statement.hashCode();
    h$ *= 1000003;
    h$ ^= (int) ((batchSize >>> 32) ^ batchSize);
    h$ *= 1000003;
    h$ ^= (optionsType == null) ? 0 : optionsType.hashCode();
    h$ *= 1000003;
    h$ ^= (label == null) ? 0 : label.hashCode();
    h$ *= 1000003;
    h$ ^= (driverConfiguration == null) ? 0 : driverConfiguration.hashCode();
    return h$;
  }

  @Override
  Neo4jIO.Write.Builder<T> toBuilder() {
    return new Builder<T>(this);
  }

  static final class Builder<T> extends Neo4jIO.Write.Builder<T> {
    private String statement;
    private Long batchSize;
    private String optionsType;
    private String label;
    private Neo4jIO.DriverConfiguration driverConfiguration;
    Builder() {
    }
    private Builder(Neo4jIO.Write<T> source) {
      this.statement = source.getStatement();
      this.batchSize = source.getBatchSize();
      this.optionsType = source.getOptionsType();
      this.label = source.getLabel();
      this.driverConfiguration = source.getDriverConfiguration();
    }
    @Override
    Neo4jIO.Write.Builder<T> setStatement(@Nullable String statement) {
      this.statement = statement;
      return this;
    }
    @Override
    Neo4jIO.Write.Builder<T> setBatchSize(long batchSize) {
      this.batchSize = batchSize;
      return this;
    }
    @Override
    Neo4jIO.Write.Builder<T> setOptionsType(@Nullable String optionsType) {
      this.optionsType = optionsType;
      return this;
    }
    @Override
    Neo4jIO.Write.Builder<T> setLabel(@Nullable String label) {
      this.label = label;
      return this;
    }
    @Override
    Neo4jIO.Write.Builder<T> setDriverConfiguration(@Nullable Neo4jIO.DriverConfiguration driverConfiguration) {
      this.driverConfiguration = driverConfiguration;
      return this;
    }
    @Override
    Neo4jIO.Write<T> build() {
      String missing = "";
      if (this.batchSize == null) {
        missing += " batchSize";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_Neo4jIO_Write<T>(
          this.statement,
          this.batchSize,
          this.optionsType,
          this.label,
          this.driverConfiguration);
    }
  }

}
