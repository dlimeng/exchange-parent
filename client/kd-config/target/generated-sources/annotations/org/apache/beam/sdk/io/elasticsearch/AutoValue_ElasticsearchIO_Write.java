

package org.apache.beam.sdk.io.elasticsearch;

import javax.annotation.Generated;
import javax.annotation.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_ElasticsearchIO_Write extends ElasticsearchIO.Write {

  private final ElasticsearchIO.ConnectionConfiguration connectionConfiguration;
  private final long maxBatchSize;
  private final long maxBatchSizeBytes;
  private final ElasticsearchIO.Write.FieldValueExtractFn idFn;
  private final ElasticsearchIO.Write.FieldValueExtractFn indexFn;
  private final ElasticsearchIO.Write.FieldValueExtractFn typeFn;
  private final ElasticsearchIO.RetryConfiguration retryConfiguration;
  private final boolean usePartialUpdate;

  private AutoValue_ElasticsearchIO_Write(
      @Nullable ElasticsearchIO.ConnectionConfiguration connectionConfiguration,
      long maxBatchSize,
      long maxBatchSizeBytes,
      @Nullable ElasticsearchIO.Write.FieldValueExtractFn idFn,
      @Nullable ElasticsearchIO.Write.FieldValueExtractFn indexFn,
      @Nullable ElasticsearchIO.Write.FieldValueExtractFn typeFn,
      @Nullable ElasticsearchIO.RetryConfiguration retryConfiguration,
      boolean usePartialUpdate) {
    this.connectionConfiguration = connectionConfiguration;
    this.maxBatchSize = maxBatchSize;
    this.maxBatchSizeBytes = maxBatchSizeBytes;
    this.idFn = idFn;
    this.indexFn = indexFn;
    this.typeFn = typeFn;
    this.retryConfiguration = retryConfiguration;
    this.usePartialUpdate = usePartialUpdate;
  }

  @Nullable
  @Override
  ElasticsearchIO.ConnectionConfiguration getConnectionConfiguration() {
    return connectionConfiguration;
  }

  @Override
  long getMaxBatchSize() {
    return maxBatchSize;
  }

  @Override
  long getMaxBatchSizeBytes() {
    return maxBatchSizeBytes;
  }

  @Nullable
  @Override
  ElasticsearchIO.Write.FieldValueExtractFn getIdFn() {
    return idFn;
  }

  @Nullable
  @Override
  ElasticsearchIO.Write.FieldValueExtractFn getIndexFn() {
    return indexFn;
  }

  @Nullable
  @Override
  ElasticsearchIO.Write.FieldValueExtractFn getTypeFn() {
    return typeFn;
  }

  @Nullable
  @Override
  ElasticsearchIO.RetryConfiguration getRetryConfiguration() {
    return retryConfiguration;
  }

  @Override
  boolean getUsePartialUpdate() {
    return usePartialUpdate;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ElasticsearchIO.Write) {
      ElasticsearchIO.Write that = (ElasticsearchIO.Write) o;
      return ((this.connectionConfiguration == null) ? (that.getConnectionConfiguration() == null) : this.connectionConfiguration.equals(that.getConnectionConfiguration()))
           && (this.maxBatchSize == that.getMaxBatchSize())
           && (this.maxBatchSizeBytes == that.getMaxBatchSizeBytes())
           && ((this.idFn == null) ? (that.getIdFn() == null) : this.idFn.equals(that.getIdFn()))
           && ((this.indexFn == null) ? (that.getIndexFn() == null) : this.indexFn.equals(that.getIndexFn()))
           && ((this.typeFn == null) ? (that.getTypeFn() == null) : this.typeFn.equals(that.getTypeFn()))
           && ((this.retryConfiguration == null) ? (that.getRetryConfiguration() == null) : this.retryConfiguration.equals(that.getRetryConfiguration()))
           && (this.usePartialUpdate == that.getUsePartialUpdate());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (connectionConfiguration == null) ? 0 : connectionConfiguration.hashCode();
    h$ *= 1000003;
    h$ ^= (int) ((maxBatchSize >>> 32) ^ maxBatchSize);
    h$ *= 1000003;
    h$ ^= (int) ((maxBatchSizeBytes >>> 32) ^ maxBatchSizeBytes);
    h$ *= 1000003;
    h$ ^= (idFn == null) ? 0 : idFn.hashCode();
    h$ *= 1000003;
    h$ ^= (indexFn == null) ? 0 : indexFn.hashCode();
    h$ *= 1000003;
    h$ ^= (typeFn == null) ? 0 : typeFn.hashCode();
    h$ *= 1000003;
    h$ ^= (retryConfiguration == null) ? 0 : retryConfiguration.hashCode();
    h$ *= 1000003;
    h$ ^= usePartialUpdate ? 1231 : 1237;
    return h$;
  }

  @Override
  ElasticsearchIO.Write.Builder builder() {
    return new Builder(this);
  }

  static final class Builder extends ElasticsearchIO.Write.Builder {
    private ElasticsearchIO.ConnectionConfiguration connectionConfiguration;
    private Long maxBatchSize;
    private Long maxBatchSizeBytes;
    private ElasticsearchIO.Write.FieldValueExtractFn idFn;
    private ElasticsearchIO.Write.FieldValueExtractFn indexFn;
    private ElasticsearchIO.Write.FieldValueExtractFn typeFn;
    private ElasticsearchIO.RetryConfiguration retryConfiguration;
    private Boolean usePartialUpdate;
    Builder() {
    }
    private Builder(ElasticsearchIO.Write source) {
      this.connectionConfiguration = source.getConnectionConfiguration();
      this.maxBatchSize = source.getMaxBatchSize();
      this.maxBatchSizeBytes = source.getMaxBatchSizeBytes();
      this.idFn = source.getIdFn();
      this.indexFn = source.getIndexFn();
      this.typeFn = source.getTypeFn();
      this.retryConfiguration = source.getRetryConfiguration();
      this.usePartialUpdate = source.getUsePartialUpdate();
    }
    @Override
    ElasticsearchIO.Write.Builder setConnectionConfiguration(@Nullable ElasticsearchIO.ConnectionConfiguration connectionConfiguration) {
      this.connectionConfiguration = connectionConfiguration;
      return this;
    }
    @Override
    ElasticsearchIO.Write.Builder setMaxBatchSize(long maxBatchSize) {
      this.maxBatchSize = maxBatchSize;
      return this;
    }
    @Override
    ElasticsearchIO.Write.Builder setMaxBatchSizeBytes(long maxBatchSizeBytes) {
      this.maxBatchSizeBytes = maxBatchSizeBytes;
      return this;
    }
    @Override
    ElasticsearchIO.Write.Builder setIdFn(@Nullable ElasticsearchIO.Write.FieldValueExtractFn idFn) {
      this.idFn = idFn;
      return this;
    }
    @Override
    ElasticsearchIO.Write.Builder setIndexFn(@Nullable ElasticsearchIO.Write.FieldValueExtractFn indexFn) {
      this.indexFn = indexFn;
      return this;
    }
    @Override
    ElasticsearchIO.Write.Builder setTypeFn(@Nullable ElasticsearchIO.Write.FieldValueExtractFn typeFn) {
      this.typeFn = typeFn;
      return this;
    }
    @Override
    ElasticsearchIO.Write.Builder setRetryConfiguration(@Nullable ElasticsearchIO.RetryConfiguration retryConfiguration) {
      this.retryConfiguration = retryConfiguration;
      return this;
    }
    @Override
    ElasticsearchIO.Write.Builder setUsePartialUpdate(boolean usePartialUpdate) {
      this.usePartialUpdate = usePartialUpdate;
      return this;
    }
    @Override
    ElasticsearchIO.Write build() {
      String missing = "";
      if (this.maxBatchSize == null) {
        missing += " maxBatchSize";
      }
      if (this.maxBatchSizeBytes == null) {
        missing += " maxBatchSizeBytes";
      }
      if (this.usePartialUpdate == null) {
        missing += " usePartialUpdate";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_ElasticsearchIO_Write(
          this.connectionConfiguration,
          this.maxBatchSize,
          this.maxBatchSizeBytes,
          this.idFn,
          this.indexFn,
          this.typeFn,
          this.retryConfiguration,
          this.usePartialUpdate);
    }
  }

}
