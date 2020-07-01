

package org.apache.beam.sdk.io.jdbc;

import javax.annotation.Generated;
import javax.annotation.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_JdbcIO_Write<T> extends JdbcIO.Write<T> {

  private final JdbcIO.DataSourceConfiguration dataSourceConfiguration;
  private final String statement;
  private final long batchSize;
  private final JdbcIO.PreparedStatementSetter<T> preparedStatementSetter;
  private final JdbcIO.RetryStrategy retryStrategy;

  private AutoValue_JdbcIO_Write(
      @Nullable JdbcIO.DataSourceConfiguration dataSourceConfiguration,
      @Nullable String statement,
      long batchSize,
      @Nullable JdbcIO.PreparedStatementSetter<T> preparedStatementSetter,
      @Nullable JdbcIO.RetryStrategy retryStrategy) {
    this.dataSourceConfiguration = dataSourceConfiguration;
    this.statement = statement;
    this.batchSize = batchSize;
    this.preparedStatementSetter = preparedStatementSetter;
    this.retryStrategy = retryStrategy;
  }

  @Nullable
  @Override
  JdbcIO.DataSourceConfiguration getDataSourceConfiguration() {
    return dataSourceConfiguration;
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
  JdbcIO.PreparedStatementSetter<T> getPreparedStatementSetter() {
    return preparedStatementSetter;
  }

  @Nullable
  @Override
  JdbcIO.RetryStrategy getRetryStrategy() {
    return retryStrategy;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof JdbcIO.Write) {
      JdbcIO.Write<?> that = (JdbcIO.Write<?>) o;
      return ((this.dataSourceConfiguration == null) ? (that.getDataSourceConfiguration() == null) : this.dataSourceConfiguration.equals(that.getDataSourceConfiguration()))
           && ((this.statement == null) ? (that.getStatement() == null) : this.statement.equals(that.getStatement()))
           && (this.batchSize == that.getBatchSize())
           && ((this.preparedStatementSetter == null) ? (that.getPreparedStatementSetter() == null) : this.preparedStatementSetter.equals(that.getPreparedStatementSetter()))
           && ((this.retryStrategy == null) ? (that.getRetryStrategy() == null) : this.retryStrategy.equals(that.getRetryStrategy()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (dataSourceConfiguration == null) ? 0 : dataSourceConfiguration.hashCode();
    h$ *= 1000003;
    h$ ^= (statement == null) ? 0 : statement.hashCode();
    h$ *= 1000003;
    h$ ^= (int) ((batchSize >>> 32) ^ batchSize);
    h$ *= 1000003;
    h$ ^= (preparedStatementSetter == null) ? 0 : preparedStatementSetter.hashCode();
    h$ *= 1000003;
    h$ ^= (retryStrategy == null) ? 0 : retryStrategy.hashCode();
    return h$;
  }

  @Override
  JdbcIO.Write.Builder<T> toBuilder() {
    return new Builder<T>(this);
  }

  static final class Builder<T> extends JdbcIO.Write.Builder<T> {
    private JdbcIO.DataSourceConfiguration dataSourceConfiguration;
    private String statement;
    private Long batchSize;
    private JdbcIO.PreparedStatementSetter<T> preparedStatementSetter;
    private JdbcIO.RetryStrategy retryStrategy;
    Builder() {
    }
    private Builder(JdbcIO.Write<T> source) {
      this.dataSourceConfiguration = source.getDataSourceConfiguration();
      this.statement = source.getStatement();
      this.batchSize = source.getBatchSize();
      this.preparedStatementSetter = source.getPreparedStatementSetter();
      this.retryStrategy = source.getRetryStrategy();
    }
    @Override
    JdbcIO.Write.Builder<T> setDataSourceConfiguration(@Nullable JdbcIO.DataSourceConfiguration dataSourceConfiguration) {
      this.dataSourceConfiguration = dataSourceConfiguration;
      return this;
    }
    @Override
    JdbcIO.Write.Builder<T> setStatement(@Nullable String statement) {
      this.statement = statement;
      return this;
    }
    @Override
    JdbcIO.Write.Builder<T> setBatchSize(long batchSize) {
      this.batchSize = batchSize;
      return this;
    }
    @Override
    JdbcIO.Write.Builder<T> setPreparedStatementSetter(@Nullable JdbcIO.PreparedStatementSetter<T> preparedStatementSetter) {
      this.preparedStatementSetter = preparedStatementSetter;
      return this;
    }
    @Override
    JdbcIO.Write.Builder<T> setRetryStrategy(@Nullable JdbcIO.RetryStrategy retryStrategy) {
      this.retryStrategy = retryStrategy;
      return this;
    }
    @Override
    JdbcIO.Write<T> build() {
      String missing = "";
      if (this.batchSize == null) {
        missing += " batchSize";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_JdbcIO_Write<T>(
          this.dataSourceConfiguration,
          this.statement,
          this.batchSize,
          this.preparedStatementSetter,
          this.retryStrategy);
    }
  }

}
