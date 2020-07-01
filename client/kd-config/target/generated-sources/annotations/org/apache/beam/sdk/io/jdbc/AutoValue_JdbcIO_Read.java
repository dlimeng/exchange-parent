

package org.apache.beam.sdk.io.jdbc;

import javax.annotation.Generated;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ValueProvider;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_JdbcIO_Read<T> extends JdbcIO.Read<T> {

  private final JdbcIO.DataSourceConfiguration dataSourceConfiguration;
  private final ValueProvider<String> query;
  private final JdbcIO.StatementPreparator statementPreparator;
  private final JdbcIO.RowMapper<T> rowMapper;
  private final Coder<T> coder;
  private final int fetchSize;

  private AutoValue_JdbcIO_Read(
      @Nullable JdbcIO.DataSourceConfiguration dataSourceConfiguration,
      @Nullable ValueProvider<String> query,
      @Nullable JdbcIO.StatementPreparator statementPreparator,
      @Nullable JdbcIO.RowMapper<T> rowMapper,
      @Nullable Coder<T> coder,
      int fetchSize) {
    this.dataSourceConfiguration = dataSourceConfiguration;
    this.query = query;
    this.statementPreparator = statementPreparator;
    this.rowMapper = rowMapper;
    this.coder = coder;
    this.fetchSize = fetchSize;
  }

  @Nullable
  @Override
  JdbcIO.DataSourceConfiguration getDataSourceConfiguration() {
    return dataSourceConfiguration;
  }

  @Nullable
  @Override
  ValueProvider<String> getQuery() {
    return query;
  }

  @Nullable
  @Override
  JdbcIO.StatementPreparator getStatementPreparator() {
    return statementPreparator;
  }

  @Nullable
  @Override
  JdbcIO.RowMapper<T> getRowMapper() {
    return rowMapper;
  }

  @Nullable
  @Override
  Coder<T> getCoder() {
    return coder;
  }

  @Override
  int getFetchSize() {
    return fetchSize;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof JdbcIO.Read) {
      JdbcIO.Read<?> that = (JdbcIO.Read<?>) o;
      return ((this.dataSourceConfiguration == null) ? (that.getDataSourceConfiguration() == null) : this.dataSourceConfiguration.equals(that.getDataSourceConfiguration()))
           && ((this.query == null) ? (that.getQuery() == null) : this.query.equals(that.getQuery()))
           && ((this.statementPreparator == null) ? (that.getStatementPreparator() == null) : this.statementPreparator.equals(that.getStatementPreparator()))
           && ((this.rowMapper == null) ? (that.getRowMapper() == null) : this.rowMapper.equals(that.getRowMapper()))
           && ((this.coder == null) ? (that.getCoder() == null) : this.coder.equals(that.getCoder()))
           && (this.fetchSize == that.getFetchSize());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (dataSourceConfiguration == null) ? 0 : dataSourceConfiguration.hashCode();
    h$ *= 1000003;
    h$ ^= (query == null) ? 0 : query.hashCode();
    h$ *= 1000003;
    h$ ^= (statementPreparator == null) ? 0 : statementPreparator.hashCode();
    h$ *= 1000003;
    h$ ^= (rowMapper == null) ? 0 : rowMapper.hashCode();
    h$ *= 1000003;
    h$ ^= (coder == null) ? 0 : coder.hashCode();
    h$ *= 1000003;
    h$ ^= fetchSize;
    return h$;
  }

  @Override
  JdbcIO.Read.Builder<T> toBuilder() {
    return new Builder<T>(this);
  }

  static final class Builder<T> extends JdbcIO.Read.Builder<T> {
    private JdbcIO.DataSourceConfiguration dataSourceConfiguration;
    private ValueProvider<String> query;
    private JdbcIO.StatementPreparator statementPreparator;
    private JdbcIO.RowMapper<T> rowMapper;
    private Coder<T> coder;
    private Integer fetchSize;
    Builder() {
    }
    private Builder(JdbcIO.Read<T> source) {
      this.dataSourceConfiguration = source.getDataSourceConfiguration();
      this.query = source.getQuery();
      this.statementPreparator = source.getStatementPreparator();
      this.rowMapper = source.getRowMapper();
      this.coder = source.getCoder();
      this.fetchSize = source.getFetchSize();
    }
    @Override
    JdbcIO.Read.Builder<T> setDataSourceConfiguration(@Nullable JdbcIO.DataSourceConfiguration dataSourceConfiguration) {
      this.dataSourceConfiguration = dataSourceConfiguration;
      return this;
    }
    @Override
    JdbcIO.Read.Builder<T> setQuery(@Nullable ValueProvider<String> query) {
      this.query = query;
      return this;
    }
    @Override
    JdbcIO.Read.Builder<T> setStatementPreparator(@Nullable JdbcIO.StatementPreparator statementPreparator) {
      this.statementPreparator = statementPreparator;
      return this;
    }
    @Override
    JdbcIO.Read.Builder<T> setRowMapper(@Nullable JdbcIO.RowMapper<T> rowMapper) {
      this.rowMapper = rowMapper;
      return this;
    }
    @Override
    JdbcIO.Read.Builder<T> setCoder(@Nullable Coder<T> coder) {
      this.coder = coder;
      return this;
    }
    @Override
    JdbcIO.Read.Builder<T> setFetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }
    @Override
    JdbcIO.Read<T> build() {
      String missing = "";
      if (this.fetchSize == null) {
        missing += " fetchSize";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_JdbcIO_Read<T>(
          this.dataSourceConfiguration,
          this.query,
          this.statementPreparator,
          this.rowMapper,
          this.coder,
          this.fetchSize);
    }
  }

}
