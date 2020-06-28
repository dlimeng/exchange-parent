

package com.knowlegene.parent.process.io.neo4j;

import javax.annotation.Generated;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.ValueProvider;
import org.neo4j.driver.v1.Driver;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_Neo4jIO_DriverConfiguration extends Neo4jIO.DriverConfiguration {

  private final Driver driver;
  private final ValueProvider<String> url;
  private final ValueProvider<String> username;
  private final ValueProvider<String> password;

  private AutoValue_Neo4jIO_DriverConfiguration(
      @Nullable Driver driver,
      @Nullable ValueProvider<String> url,
      @Nullable ValueProvider<String> username,
      @Nullable ValueProvider<String> password) {
    this.driver = driver;
    this.url = url;
    this.username = username;
    this.password = password;
  }

  @Nullable
  @Override
  Driver getDriver() {
    return driver;
  }

  @Nullable
  @Override
  ValueProvider<String> getUrl() {
    return url;
  }

  @Nullable
  @Override
  ValueProvider<String> getUsername() {
    return username;
  }

  @Nullable
  @Override
  ValueProvider<String> getPassword() {
    return password;
  }

  @Override
  public String toString() {
    return "DriverConfiguration{"
         + "driver=" + driver + ", "
         + "url=" + url + ", "
         + "username=" + username + ", "
         + "password=" + password
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Neo4jIO.DriverConfiguration) {
      Neo4jIO.DriverConfiguration that = (Neo4jIO.DriverConfiguration) o;
      return ((this.driver == null) ? (that.getDriver() == null) : this.driver.equals(that.getDriver()))
           && ((this.url == null) ? (that.getUrl() == null) : this.url.equals(that.getUrl()))
           && ((this.username == null) ? (that.getUsername() == null) : this.username.equals(that.getUsername()))
           && ((this.password == null) ? (that.getPassword() == null) : this.password.equals(that.getPassword()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (driver == null) ? 0 : driver.hashCode();
    h$ *= 1000003;
    h$ ^= (url == null) ? 0 : url.hashCode();
    h$ *= 1000003;
    h$ ^= (username == null) ? 0 : username.hashCode();
    h$ *= 1000003;
    h$ ^= (password == null) ? 0 : password.hashCode();
    return h$;
  }

  @Override
  Neo4jIO.DriverConfiguration.Builder builder() {
    return new Builder(this);
  }

  static final class Builder extends Neo4jIO.DriverConfiguration.Builder {
    private Driver driver;
    private ValueProvider<String> url;
    private ValueProvider<String> username;
    private ValueProvider<String> password;
    Builder() {
    }
    private Builder(Neo4jIO.DriverConfiguration source) {
      this.driver = source.getDriver();
      this.url = source.getUrl();
      this.username = source.getUsername();
      this.password = source.getPassword();
    }
    @Override
    Neo4jIO.DriverConfiguration.Builder setDriver(@Nullable Driver driver) {
      this.driver = driver;
      return this;
    }
    @Override
    Neo4jIO.DriverConfiguration.Builder setUrl(@Nullable ValueProvider<String> url) {
      this.url = url;
      return this;
    }
    @Override
    Neo4jIO.DriverConfiguration.Builder setUsername(@Nullable ValueProvider<String> username) {
      this.username = username;
      return this;
    }
    @Override
    Neo4jIO.DriverConfiguration.Builder setPassword(@Nullable ValueProvider<String> password) {
      this.password = password;
      return this;
    }
    @Override
    Neo4jIO.DriverConfiguration build() {
      return new AutoValue_Neo4jIO_DriverConfiguration(
          this.driver,
          this.url,
          this.username,
          this.password);
    }
  }

}
