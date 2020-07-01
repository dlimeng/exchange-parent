

package org.apache.beam.sdk.io.elasticsearch;

import javax.annotation.Generated;
import org.joda.time.Duration;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_ElasticsearchIO_RetryConfiguration extends ElasticsearchIO.RetryConfiguration {

  private final int maxAttempts;
  private final Duration maxDuration;
  private final ElasticsearchIO.RetryConfiguration.RetryPredicate retryPredicate;

  private AutoValue_ElasticsearchIO_RetryConfiguration(
      int maxAttempts,
      Duration maxDuration,
      ElasticsearchIO.RetryConfiguration.RetryPredicate retryPredicate) {
    this.maxAttempts = maxAttempts;
    this.maxDuration = maxDuration;
    this.retryPredicate = retryPredicate;
  }

  @Override
  int getMaxAttempts() {
    return maxAttempts;
  }

  @Override
  Duration getMaxDuration() {
    return maxDuration;
  }

  @Override
  ElasticsearchIO.RetryConfiguration.RetryPredicate getRetryPredicate() {
    return retryPredicate;
  }

  @Override
  public String toString() {
    return "RetryConfiguration{"
         + "maxAttempts=" + maxAttempts + ", "
         + "maxDuration=" + maxDuration + ", "
         + "retryPredicate=" + retryPredicate
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ElasticsearchIO.RetryConfiguration) {
      ElasticsearchIO.RetryConfiguration that = (ElasticsearchIO.RetryConfiguration) o;
      return (this.maxAttempts == that.getMaxAttempts())
           && (this.maxDuration.equals(that.getMaxDuration()))
           && (this.retryPredicate.equals(that.getRetryPredicate()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= maxAttempts;
    h$ *= 1000003;
    h$ ^= maxDuration.hashCode();
    h$ *= 1000003;
    h$ ^= retryPredicate.hashCode();
    return h$;
  }

  @Override
  ElasticsearchIO.RetryConfiguration.Builder builder() {
    return new Builder(this);
  }

  static final class Builder extends ElasticsearchIO.RetryConfiguration.Builder {
    private Integer maxAttempts;
    private Duration maxDuration;
    private ElasticsearchIO.RetryConfiguration.RetryPredicate retryPredicate;
    Builder() {
    }
    private Builder(ElasticsearchIO.RetryConfiguration source) {
      this.maxAttempts = source.getMaxAttempts();
      this.maxDuration = source.getMaxDuration();
      this.retryPredicate = source.getRetryPredicate();
    }
    @Override
    ElasticsearchIO.RetryConfiguration.Builder setMaxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }
    @Override
    ElasticsearchIO.RetryConfiguration.Builder setMaxDuration(Duration maxDuration) {
      if (maxDuration == null) {
        throw new NullPointerException("Null maxDuration");
      }
      this.maxDuration = maxDuration;
      return this;
    }
    @Override
    ElasticsearchIO.RetryConfiguration.Builder setRetryPredicate(ElasticsearchIO.RetryConfiguration.RetryPredicate retryPredicate) {
      if (retryPredicate == null) {
        throw new NullPointerException("Null retryPredicate");
      }
      this.retryPredicate = retryPredicate;
      return this;
    }
    @Override
    ElasticsearchIO.RetryConfiguration build() {
      String missing = "";
      if (this.maxAttempts == null) {
        missing += " maxAttempts";
      }
      if (this.maxDuration == null) {
        missing += " maxDuration";
      }
      if (this.retryPredicate == null) {
        missing += " retryPredicate";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_ElasticsearchIO_RetryConfiguration(
          this.maxAttempts,
          this.maxDuration,
          this.retryPredicate);
    }
  }

}
