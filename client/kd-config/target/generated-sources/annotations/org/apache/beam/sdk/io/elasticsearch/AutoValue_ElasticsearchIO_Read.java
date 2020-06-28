

package org.apache.beam.sdk.io.elasticsearch;

import javax.annotation.Generated;
import javax.annotation.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_ElasticsearchIO_Read extends ElasticsearchIO.Read {

  private final ElasticsearchIO.ConnectionConfiguration connectionConfiguration;
  private final String query;
  private final boolean withMetadata;
  private final String scrollKeepalive;
  private final long batchSize;

  private AutoValue_ElasticsearchIO_Read(
      @Nullable ElasticsearchIO.ConnectionConfiguration connectionConfiguration,
      @Nullable String query,
      boolean withMetadata,
      String scrollKeepalive,
      long batchSize) {
    this.connectionConfiguration = connectionConfiguration;
    this.query = query;
    this.withMetadata = withMetadata;
    this.scrollKeepalive = scrollKeepalive;
    this.batchSize = batchSize;
  }

  @Nullable
  @Override
  ElasticsearchIO.ConnectionConfiguration getConnectionConfiguration() {
    return connectionConfiguration;
  }

  @Nullable
  @Override
  String getQuery() {
    return query;
  }

  @Override
  boolean isWithMetadata() {
    return withMetadata;
  }

  @Override
  String getScrollKeepalive() {
    return scrollKeepalive;
  }

  @Override
  long getBatchSize() {
    return batchSize;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ElasticsearchIO.Read) {
      ElasticsearchIO.Read that = (ElasticsearchIO.Read) o;
      return ((this.connectionConfiguration == null) ? (that.getConnectionConfiguration() == null) : this.connectionConfiguration.equals(that.getConnectionConfiguration()))
           && ((this.query == null) ? (that.getQuery() == null) : this.query.equals(that.getQuery()))
           && (this.withMetadata == that.isWithMetadata())
           && (this.scrollKeepalive.equals(that.getScrollKeepalive()))
           && (this.batchSize == that.getBatchSize());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (connectionConfiguration == null) ? 0 : connectionConfiguration.hashCode();
    h$ *= 1000003;
    h$ ^= (query == null) ? 0 : query.hashCode();
    h$ *= 1000003;
    h$ ^= withMetadata ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= scrollKeepalive.hashCode();
    h$ *= 1000003;
    h$ ^= (int) ((batchSize >>> 32) ^ batchSize);
    return h$;
  }

  @Override
  ElasticsearchIO.Read.Builder builder() {
    return new Builder(this);
  }

  static final class Builder extends ElasticsearchIO.Read.Builder {
    private ElasticsearchIO.ConnectionConfiguration connectionConfiguration;
    private String query;
    private Boolean withMetadata;
    private String scrollKeepalive;
    private Long batchSize;
    Builder() {
    }
    private Builder(ElasticsearchIO.Read source) {
      this.connectionConfiguration = source.getConnectionConfiguration();
      this.query = source.getQuery();
      this.withMetadata = source.isWithMetadata();
      this.scrollKeepalive = source.getScrollKeepalive();
      this.batchSize = source.getBatchSize();
    }
    @Override
    ElasticsearchIO.Read.Builder setConnectionConfiguration(@Nullable ElasticsearchIO.ConnectionConfiguration connectionConfiguration) {
      this.connectionConfiguration = connectionConfiguration;
      return this;
    }
    @Override
    ElasticsearchIO.Read.Builder setQuery(@Nullable String query) {
      this.query = query;
      return this;
    }
    @Override
    ElasticsearchIO.Read.Builder setWithMetadata(boolean withMetadata) {
      this.withMetadata = withMetadata;
      return this;
    }
    @Override
    ElasticsearchIO.Read.Builder setScrollKeepalive(String scrollKeepalive) {
      if (scrollKeepalive == null) {
        throw new NullPointerException("Null scrollKeepalive");
      }
      this.scrollKeepalive = scrollKeepalive;
      return this;
    }
    @Override
    ElasticsearchIO.Read.Builder setBatchSize(long batchSize) {
      this.batchSize = batchSize;
      return this;
    }
    @Override
    ElasticsearchIO.Read build() {
      String missing = "";
      if (this.withMetadata == null) {
        missing += " withMetadata";
      }
      if (this.scrollKeepalive == null) {
        missing += " scrollKeepalive";
      }
      if (this.batchSize == null) {
        missing += " batchSize";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_ElasticsearchIO_Read(
          this.connectionConfiguration,
          this.query,
          this.withMetadata,
          this.scrollKeepalive,
          this.batchSize);
    }
  }

}
