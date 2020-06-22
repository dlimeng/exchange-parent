package com.knowlegene.parent.process.io.neo4j;

import com.google.auto.value.AutoValue;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.common.constantenum.Neo4jEnum;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jObject;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.neo4j.driver.v1.*;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.neo4j.driver.v1.Values.parameters;

/**
 * @Author: limeng
 * @Date: 2019/9/23 17:12
 */
public class Neo4jIO {
    private static final Logger LOG = LoggerFactory.getLogger(Neo4jIO.class);

    private static final long DEFAULT_BATCH_SIZE = 1000L;
    private static final int DEFAULT_FETCH_SIZE = 50_000;

    private Neo4jIO() {}

    public static <T>Write<T> write(){
        return new AutoValue_Neo4jIO_Write.Builder<T>()
                .setBatchSize(DEFAULT_BATCH_SIZE)
                .build();
    }

    @AutoValue
    public abstract static class DriverConfiguration implements Serializable {
        @Nullable
        abstract  Driver getDriver();

        abstract  Builder builder();

        @Nullable
        abstract ValueProvider<String> getUrl();

        @Nullable
        abstract ValueProvider<String> getUsername();

        @Nullable
        abstract ValueProvider<String> getPassword();

        @AutoValue.Builder
        abstract static class Builder{
            abstract  Builder setDriver(Driver driver);
            abstract  Builder setUrl(ValueProvider<String> url);
            abstract Builder setUsername(ValueProvider<String> username);
            abstract Builder setPassword(ValueProvider<String> password);
            abstract DriverConfiguration build();
        }


        public static DriverConfiguration create(Driver driver){
            checkArgument(driver != null, "driver can not be null");
            checkArgument(driver instanceof Serializable, "driver must be Serializable");
            return  new AutoValue_Neo4jIO_DriverConfiguration.Builder().setDriver(driver).build();
        }

        public static DriverConfiguration create(String url,String username,String password){
            checkArgument(url != null, "url can not be null");
            checkArgument(username != null, "username can not be null");
            checkArgument(password != null, "password can not be null");
            return  new AutoValue_Neo4jIO_DriverConfiguration.Builder()
                    .setUrl(ValueProvider.StaticValueProvider.of(url))
                    .setUsername(ValueProvider.StaticValueProvider.of(username))
                    .setPassword(ValueProvider.StaticValueProvider.of(password))
                    .build();
        }

        private void populateDisplayData(DisplayData.Builder builder) {
            if(getDriver() != null){
                builder.addIfNotNull(DisplayData.item("driver", getDriver().getClass().getName()));
            }else{
                builder.addIfNotNull(DisplayData.item("url", getUrl()));
                builder.addIfNotNull(DisplayData.item("username", getUsername()));
                builder.addIfNotNull(DisplayData.item("password", getUsername()));
            }
        }

        Driver buildDriver(){
            Driver driver=null;
            if(getDriver() != null){
                driver = getDriver();
            }else{
                if(getUrl() != null && getUsername()!=null && getPassword()!=null){
                    driver = GraphDatabase.driver( getUrl().get(), AuthTokens.basic( getUsername().get(), getPassword().get()));
                }
            }
            return driver;
        }

    }


    @AutoValue
    public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

        @Nullable
        abstract String getStatement();

        abstract long getBatchSize();
        @Nullable
        abstract String getOptionsType();

        @Nullable
        abstract String getLabel();

        abstract Builder<T> toBuilder();
        @Nullable
        abstract DriverConfiguration getDriverConfiguration();

        @AutoValue.Builder
        abstract static class Builder<T>{
            abstract Builder<T> setDriverConfiguration(DriverConfiguration config);
            abstract Builder<T> setStatement(String statement);
            abstract Builder<T> setBatchSize(long batchSize);
            abstract Builder<T> setOptionsType(String optionsType);
            abstract Builder<T> setLabel(String label);
            abstract Write<T> build();
        }

        public Write<T> withLabel(String label){
            checkArgument(label != null, "label can not be null");
            return toBuilder().setLabel(label).build();
        }

        public Write<T> withOptionsType(String optionsType){
            checkArgument(optionsType != null, "optionsType can not be null");
            return toBuilder().setOptionsType(optionsType).build();
        }

        public Write<T> withDriverConfiguration(DriverConfiguration config){
            checkArgument(config != null, "driverConfiguration can not be null");
            checkArgument(config instanceof Serializable, "driverConfiguration must be Serializable");
            return toBuilder().setDriverConfiguration(config).build();
        }

        public Write<T> withStatement(String statement){
            checkArgument(statement != null, "optionsType can not be null");
            return toBuilder().setStatement(statement).build();
        }
        public Write<T> withBatchSize(long batchSize){
            checkArgument(batchSize > 0, "batchSize must be > 0, but was %s", batchSize);
            return toBuilder().setBatchSize(batchSize).build();
        }

        @Override
        public PDone expand(PCollection<T> input) {
            checkArgument(
                    getDriverConfiguration() != null, "withDriverConfiguration() is required");
            checkArgument(getStatement() != null, "withStatement() is required");

            input.apply(ParDo.of(new WriteFn<T>(this)));
            return PDone.in(input.getPipeline());
        }
    }


    private static class WriteFn<T> extends DoFn<T,Void> {
        private final Write spec;
        private Driver driver;
        private Session session;
        private List<T> records = new ArrayList<>();
        private static final int MAX_RETRIES = 5;
        private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
                FluentBackoff.DEFAULT
                        .withMaxRetries(MAX_RETRIES)
                        .withInitialBackoff(Duration.standardSeconds(5));

        public WriteFn(Write spec) {
            this.spec = spec;
        }

        @Setup
        public void setup()  throws Exception {
            driver = spec.getDriverConfiguration().buildDriver();
        }

        @StartBundle
        public void startBundle() throws Exception {
            session = driver.session();
        }

        @ProcessElement
        public void processElement(ProcessContext context) throws Exception {
            T element = context.element();
            records.add(element);
            if(records.size() >= spec.getBatchSize() ){
                executeBatch();
            }
        }

        @FinishBundle
        public void finishBundle() throws Exception {
            executeBatch();
            try {
                if(session != null){
                    session.close();
                }
            }catch (Exception e){
                LOG.warn(e.getMessage());
            }

        }

        private void executeBatch() throws IOException, InterruptedException {
            if (records.isEmpty()) {
                return;
            }
            Sleeper sleeper = Sleeper.DEFAULT;
            BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();
            while (true){
                try (Transaction tx = session.beginTransaction()){
                    try {
                        for(T record:records){
                            Value value = processRecord(record);
                            String dsl = getDSL(value);
                            tx.run(dsl,value);
                        }
                        tx.success();
                        break;
                    }catch (Exception exception){
                        LOG.warn("Deadlock detected, retrying", exception);
                        tx.failure();
                        if (!BackOffUtils.next(sleeper, backoff)) {
                            // we tried the max number of times
                            throw exception;
                        }
                    }
                }
            }

            records.clear();
        }

        private Value processRecord(final T record) {
            try {
                if(record instanceof Neo4jObject){
                    Object[] objectValue = ((Neo4jObject) record).getObjectValue();
                    return parameters(objectValue);
                }
                return parameters(record);
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        }

        private String getDSL(Value value){
            String optionsType = spec.getOptionsType();
            String statement = spec.getStatement();
            String result=statement;
            if(BaseUtil.isNotBlank(optionsType) && BaseUtil.isNotBlank(statement)){
                if(optionsType.equals(Neo4jEnum.RELATE.getName())){
                    Value value1 = value.get(spec.getLabel());
                    if(value1 != null){
                        result = String.format(statement,value1.asString());
                    }
                }
            }
            return result;
        }


        @Teardown
        public void teardown() throws Exception {
            if (driver instanceof AutoCloseable) {
                ((AutoCloseable) driver).close();
            }
        }

    }
}
