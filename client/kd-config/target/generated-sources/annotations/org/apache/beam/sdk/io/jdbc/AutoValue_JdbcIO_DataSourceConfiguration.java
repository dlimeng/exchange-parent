

package org.apache.beam.sdk.io.jdbc;

import javax.annotation.Generated;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.options.ValueProvider;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_JdbcIO_DataSourceConfiguration extends JdbcIO.DataSourceConfiguration {

  private final ValueProvider<String> driverClassName;
  private final ValueProvider<String> url;
  private final ValueProvider<String> username;
  private final ValueProvider<String> password;
  private final ValueProvider<String> connectionProperties;
  private final DataSource dataSource;

  private AutoValue_JdbcIO_DataSourceConfiguration(
      @Nullable ValueProvider<String> driverClassName,
      @Nullable ValueProvider<String> url,
      @Nullable ValueProvider<String> username,
      @Nullable ValueProvider<String> password,
      @Nullable ValueProvider<String> connectionProperties,
      @Nullable DataSource dataSource) {
    this.driverClassName = driverClassName;
    this.url = url;
    this.username = username;
    this.password = password;
    this.connectionProperties = connectionProperties;
    this.dataSource = dataSource;
  }

  @Nullable
  @Override
  ValueProvider<String> getDriverClassName() {
    return driverClassName;
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

  @Nullable
  @Override
  ValueProvider<String> getConnectionProperties() {
    return connectionProperties;
  }

  @Nullable
  @Override
  DataSource getDataSource() {
    return dataSource;
  }

  @Override
  public String toString() {
    return "DataSourceConfiguration{"
         + "driverClassName=" + driverClassName + ", "
         + "url=" + url + ", "
         + "username=" + username + ", "
         + "password=" + password + ", "
         + "connectionProperties=" + connectionProperties + ", "
         + "dataSource=" + dataSource
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof JdbcIO.DataSourceConfiguration) {
      JdbcIO.DataSourceConfiguration that = (JdbcIO.DataSourceConfiguration) o;
      return ((this.driverClassName == null) ? (that.getDriverClassName() == null) : this.driverClassName.equals(that.getDriverClassName()))
           && ((this.url == null) ? (that.getUrl() == null) : this.url.equals(that.getUrl()))
           && ((this.username == null) ? (that.getUsername() == null) : this.username.equals(that.getUsername()))
           && ((this.password == null) ? (that.getPassword() == null) : this.password.equals(that.getPassword()))
           && ((this.connectionProperties == null) ? (that.getConnectionProperties() == null) : this.connectionProperties.equals(that.getConnectionProperties()))
           && ((this.dataSource == null) ? (that.getDataSource() == null) : this.dataSource.equals(that.getDataSource()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (driverClassName == null) ? 0 : driverClassName.hashCode();
    h$ *= 1000003;
    h$ ^= (url == null) ? 0 : url.hashCode();
    h$ *= 1000003;
    h$ ^= (username == null) ? 0 : username.hashCode();
    h$ *= 1000003;
    h$ ^= (password == null) ? 0 : password.hashCode();
    h$ *= 1000003;
    h$ ^= (connectionProperties == null) ? 0 : connectionProperties.hashCode();
    h$ *= 1000003;
    h$ ^= (dataSource == null) ? 0 : dataSource.hashCode();
    return h$;
  }

  @Override
  JdbcIO.DataSourceConfiguration.Builder builder() {
    return new Builder(this);
  }

  static final class Builder extends JdbcIO.DataSourceConfiguration.Builder {
    private ValueProvider<String> driverClassName;
    private ValueProvider<String> url;
    private ValueProvider<String> username;
    private ValueProvider<String> password;
    private ValueProvider<String> connectionProperties;
    private DataSource dataSource;
    Builder() {
    }
    private Builder(JdbcIO.DataSourceConfiguration source) {
      this.driverClassName = source.getDriverClassName();
      this.url = source.getUrl();
      this.username = source.getUsername();
      this.password = source.getPassword();
      this.connectionProperties = source.getConnectionProperties();
      this.dataSource = source.getDataSource();
    }
    @Override
    JdbcIO.DataSourceConfiguration.Builder setDriverClassName(@Nullable ValueProvider<String> driverClassName) {
      this.driverClassName = driverClassName;
      return this;
    }
    @Override
    JdbcIO.DataSourceConfiguration.Builder setUrl(@Nullable ValueProvider<String> url) {
      this.url = url;
      return this;
    }
    @Override
    JdbcIO.DataSourceConfiguration.Builder setUsername(@Nullable ValueProvider<String> username) {
      this.username = username;
      return this;
    }
    @Override
    JdbcIO.DataSourceConfiguration.Builder setPassword(@Nullable ValueProvider<String> password) {
      this.password = password;
      return this;
    }
    @Override
    JdbcIO.DataSourceConfiguration.Builder setConnectionProperties(@Nullable ValueProvider<String> connectionProperties) {
      this.connectionProperties = connectionProperties;
      return this;
    }
    @Override
    JdbcIO.DataSourceConfiguration.Builder setDataSource(@Nullable DataSource dataSource) {
      this.dataSource = dataSource;
      return this;
    }
    @Override
    JdbcIO.DataSourceConfiguration build() {
      return new AutoValue_JdbcIO_DataSourceConfiguration(
          this.driverClassName,
          this.url,
          this.username,
          this.password,
          this.connectionProperties,
          this.dataSource);
    }
  }

}
