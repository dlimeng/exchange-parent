

package org.apache.beam.sdk.io.elasticsearch;

import java.util.List;
import javax.annotation.Generated;
import javax.annotation.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_ElasticsearchIO_ConnectionConfiguration extends ElasticsearchIO.ConnectionConfiguration {

  private final List<String> addresses;
  private final String username;
  private final String password;
  private final String keystorePath;
  private final String keystorePassword;
  private final String index;
  private final String type;

  private AutoValue_ElasticsearchIO_ConnectionConfiguration(
      List<String> addresses,
      @Nullable String username,
      @Nullable String password,
      @Nullable String keystorePath,
      @Nullable String keystorePassword,
      String index,
      String type) {
    this.addresses = addresses;
    this.username = username;
    this.password = password;
    this.keystorePath = keystorePath;
    this.keystorePassword = keystorePassword;
    this.index = index;
    this.type = type;
  }

  @Override
  public List<String> getAddresses() {
    return addresses;
  }

  @Nullable
  @Override
  public String getUsername() {
    return username;
  }

  @Nullable
  @Override
  public String getPassword() {
    return password;
  }

  @Nullable
  @Override
  public String getKeystorePath() {
    return keystorePath;
  }

  @Nullable
  @Override
  public String getKeystorePassword() {
    return keystorePassword;
  }

  @Override
  public String getIndex() {
    return index;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public String toString() {
    return "ConnectionConfiguration{"
         + "addresses=" + addresses + ", "
         + "username=" + username + ", "
         + "password=" + password + ", "
         + "keystorePath=" + keystorePath + ", "
         + "keystorePassword=" + keystorePassword + ", "
         + "index=" + index + ", "
         + "type=" + type
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ElasticsearchIO.ConnectionConfiguration) {
      ElasticsearchIO.ConnectionConfiguration that = (ElasticsearchIO.ConnectionConfiguration) o;
      return (this.addresses.equals(that.getAddresses()))
           && ((this.username == null) ? (that.getUsername() == null) : this.username.equals(that.getUsername()))
           && ((this.password == null) ? (that.getPassword() == null) : this.password.equals(that.getPassword()))
           && ((this.keystorePath == null) ? (that.getKeystorePath() == null) : this.keystorePath.equals(that.getKeystorePath()))
           && ((this.keystorePassword == null) ? (that.getKeystorePassword() == null) : this.keystorePassword.equals(that.getKeystorePassword()))
           && (this.index.equals(that.getIndex()))
           && (this.type.equals(that.getType()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= addresses.hashCode();
    h$ *= 1000003;
    h$ ^= (username == null) ? 0 : username.hashCode();
    h$ *= 1000003;
    h$ ^= (password == null) ? 0 : password.hashCode();
    h$ *= 1000003;
    h$ ^= (keystorePath == null) ? 0 : keystorePath.hashCode();
    h$ *= 1000003;
    h$ ^= (keystorePassword == null) ? 0 : keystorePassword.hashCode();
    h$ *= 1000003;
    h$ ^= index.hashCode();
    h$ *= 1000003;
    h$ ^= type.hashCode();
    return h$;
  }

  @Override
  ElasticsearchIO.ConnectionConfiguration.Builder builder() {
    return new Builder(this);
  }

  static final class Builder extends ElasticsearchIO.ConnectionConfiguration.Builder {
    private List<String> addresses;
    private String username;
    private String password;
    private String keystorePath;
    private String keystorePassword;
    private String index;
    private String type;
    Builder() {
    }
    private Builder(ElasticsearchIO.ConnectionConfiguration source) {
      this.addresses = source.getAddresses();
      this.username = source.getUsername();
      this.password = source.getPassword();
      this.keystorePath = source.getKeystorePath();
      this.keystorePassword = source.getKeystorePassword();
      this.index = source.getIndex();
      this.type = source.getType();
    }
    @Override
    ElasticsearchIO.ConnectionConfiguration.Builder setAddresses(List<String> addresses) {
      if (addresses == null) {
        throw new NullPointerException("Null addresses");
      }
      this.addresses = addresses;
      return this;
    }
    @Override
    ElasticsearchIO.ConnectionConfiguration.Builder setUsername(@Nullable String username) {
      this.username = username;
      return this;
    }
    @Override
    ElasticsearchIO.ConnectionConfiguration.Builder setPassword(@Nullable String password) {
      this.password = password;
      return this;
    }
    @Override
    ElasticsearchIO.ConnectionConfiguration.Builder setKeystorePath(@Nullable String keystorePath) {
      this.keystorePath = keystorePath;
      return this;
    }
    @Override
    ElasticsearchIO.ConnectionConfiguration.Builder setKeystorePassword(@Nullable String keystorePassword) {
      this.keystorePassword = keystorePassword;
      return this;
    }
    @Override
    ElasticsearchIO.ConnectionConfiguration.Builder setIndex(String index) {
      if (index == null) {
        throw new NullPointerException("Null index");
      }
      this.index = index;
      return this;
    }
    @Override
    ElasticsearchIO.ConnectionConfiguration.Builder setType(String type) {
      if (type == null) {
        throw new NullPointerException("Null type");
      }
      this.type = type;
      return this;
    }
    @Override
    ElasticsearchIO.ConnectionConfiguration build() {
      String missing = "";
      if (this.addresses == null) {
        missing += " addresses";
      }
      if (this.index == null) {
        missing += " index";
      }
      if (this.type == null) {
        missing += " type";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_ElasticsearchIO_ConnectionConfiguration(
          this.addresses,
          this.username,
          this.password,
          this.keystorePath,
          this.keystorePassword,
          this.index,
          this.type);
    }
  }

}
