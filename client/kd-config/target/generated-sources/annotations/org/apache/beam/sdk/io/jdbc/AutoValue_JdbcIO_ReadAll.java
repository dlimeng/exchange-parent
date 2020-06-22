

package org.apache.beam.sdk.io.jdbc;

import javax.annotation.Generated;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ValueProvider;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_JdbcIO_ReadAll<ParameterT, OutputT> extends JdbcIO.ReadAll<ParameterT, OutputT> {

  private final JdbcIO.DataSourceConfiguration dataSourceConfiguration;
  private final ValueProvider<String> query;
  private final JdbcIO.PreparedStatementSetter<ParameterT> parameterSetter;
  private final JdbcIO.RowMapper<OutputT> rowMapper;
  private final Coder<OutputT> coder;
  private final int fetchSize;

  private AutoValue_JdbcIO_ReadAll(
      @Nullable JdbcIO.DataSourceConfiguration dataSourceConfiguration,
      @Nullable ValueProvider<String> query,
      @Nullable JdbcIO.PreparedStatementSetter<ParameterT> parameterSetter,
      @Nullable JdbcIO.RowMapper<OutputT> rowMapper,
      @Nullable Coder<OutputT> coder,
      int fetchSize) {
    this.dataSourceConfiguration = dataSourceConfiguration;
    this.query = query;
    this.parameterSetter = parameterSetter;
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
  JdbcIO.PreparedStatementSetter<ParameterT> getParameterSetter() {
    return parameterSetter;
  }

  @Nullable
  @Override
  JdbcIO.RowMapper<OutputT> getRowMapper() {
    return rowMapper;
  }

  @Nullable
  @Override
  Coder<OutputT> getCoder() {
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
    if (o instanceof JdbcIO.ReadAll) {
      JdbcIO.ReadAll<?, ?> that = (JdbcIO.ReadAll<?, ?>) o;
      return ((this.dataSourceConfiguration == null) ? (that.getDataSourceConfiguration() == null) : this.dataSourceConfiguration.equals(that.getDataSourceConfiguration()))
           && ((this.query == null) ? (that.getQuery() == null) : this.query.equals(that.getQuery()))
           && ((this.parameterSetter == null) ? (that.getParameterSetter() == null) : this.parameterSetter.equals(that.getParameterSetter()))
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
    h$ ^= (parameterSetter == null) ? 0 : parameterSetter.hashCode();
    h$ *= 1000003;
    h$ ^= (rowMapper == null) ? 0 : rowMapper.hashCode();
    h$ *= 1000003;
    h$ ^= (coder == null) ? 0 : coder.hashCode();
    h$ *= 1000003;
    h$ ^= fetchSize;
    return h$;
  }

  @Override
  JdbcIO.ReadAll.Builder<ParameterT, OutputT> toBuilder() {
    return new Builder<ParameterT, OutputT>(this);
  }

  static final class Builder<ParameterT, OutputT> extends JdbcIO.ReadAll.Builder<ParameterT, OutputT> {
    private JdbcIO.DataSourceConfiguration dataSourceConfiguration;
    private ValueProvider<String> query;
    private JdbcIO.PreparedStatementSetter<ParameterT> parameterSetter;
    private JdbcIO.RowMapper<OutputT> rowMapper;
    private Coder<OutputT> coder;
    private Integer fetchSize;
    Builder() {
    }
    private Builder(JdbcIO.ReadAll<ParameterT, OutputT> source) {
      this.dataSourceConfiguration = source.getDataSourceConfiguration();
      this.query = source.getQuery();
      this.parameterSetter = source.getParameterSetter();
      this.rowMapper = source.getRowMapper();
      this.coder = source.getCoder();
      this.fetchSize = source.getFetchSize();
    }
    @Override
    JdbcIO.ReadAll.Builder<ParameterT, OutputT> setDataSourceConfiguration(@Nullable JdbcIO.DataSourceConfiguration dataSourceConfiguration) {
      this.dataSourceConfiguration = dataSourceConfiguration;
      return this;
    }
    @Override
    JdbcIO.ReadAll.Builder<ParameterT, OutputT> setQuery(@Nullable ValueProvider<String> query) {
      this.query = query;
      return this;
    }
    @Override
    JdbcIO.ReadAll.Builder<ParameterT, OutputT> setParameterSetter(@Nullable JdbcIO.PreparedStatementSetter<ParameterT> parameterSetter) {
      this.parameterSetter = parameterSetter;
      return this;
    }
    @Override
    JdbcIO.ReadAll.Builder<ParameterT, OutputT> setRowMapper(@Nullable JdbcIO.RowMapper<OutputT> rowMapper) {
      this.rowMapper = rowMapper;
      return this;
    }
    @Override
    JdbcIO.ReadAll.Builder<ParameterT, OutputT> setCoder(@Nullable Coder<OutputT> coder) {
      this.coder = coder;
      return this;
    }
    @Override
    JdbcIO.ReadAll.Builder<ParameterT, OutputT> setFetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }
    @Override
    JdbcIO.ReadAll<ParameterT, OutputT> build() {
      String missing = "";
      if (this.fetchSize == null) {
        missing += " fetchSize";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_JdbcIO_ReadAll<ParameterT, OutputT>(
          this.dataSourceConfiguration,
          this.query,
          this.parameterSetter,
          this.rowMapper,
          this.coder,
          this.fetchSize);
    }
  }

}
