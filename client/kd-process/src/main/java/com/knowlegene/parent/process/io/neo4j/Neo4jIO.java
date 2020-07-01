package com.knowlegene.parent.process.io.neo4j;

import com.google.auto.value.AutoValue;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.common.constantenum.Neo4jEnum;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jObject;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.neo4j.driver.v1.Values;
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

    private static final long DEFAULT_BATCH_SIZE = 4096L;
    private static final int DEFAULT_FETCH_SIZE = 50_000;

    private Neo4jIO() {}


    public static <T>  Read<T> read(){
        return new AutoValue_Neo4jIO_Read.Builder<T>().build();
    }

    public static <ParameterT, OutputT> ReadAll<ParameterT, OutputT> readAll() {
        return new AutoValue_Neo4jIO_ReadAll.Builder<ParameterT, OutputT>().build();
    }

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
                    driver = GraphDatabase.driver(getUrl().get(), AuthTokens.basic( getUsername().get(), getPassword().get()));
                }
            }
            return driver;
        }

    }

    @FunctionalInterface
    public interface RowMapper<T> extends Serializable {
        T mapRow(StatementResult resultSet) throws Exception;
    }

    @AutoValue
    public abstract static class Read<T> extends  PTransform<PBegin, PCollection<T>>{
        @Nullable
        abstract DriverConfiguration getDriverConfiguration();
        @Nullable
        abstract ValueProvider<String> getQuery();
        @Nullable
        abstract Coder<T> getCoder();

        @Nullable
        abstract RowMapper<T> getRowMapper();

        abstract Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T>{
            abstract Builder<T> setDriverConfiguration(DriverConfiguration config);
            abstract Builder<T> setQuery(ValueProvider<String> query);
            abstract Builder<T> setCoder(Coder<T> coder);
            abstract Builder<T> setRowMapper(RowMapper<T> rowMapper);
            abstract Read<T> build();
        }

        public Read<T>  withDriverConfiguration(DriverConfiguration config){
            checkArgument(config != null, "driverConfiguration can not be null");
            checkArgument(config instanceof Serializable, "driverConfiguration must be Serializable");
            return toBuilder().setDriverConfiguration(config).build();
        }

        public Read<T> withQuery(String query){
            checkArgument(query != null, "query can not be null");
            return withQuery(ValueProvider.StaticValueProvider.of(query));
        }

        public Read<T> withQuery(ValueProvider<String> query){
            checkArgument(query != null, "query can not be null");
            return toBuilder().setQuery(query).build();
        }

        public Read<T> withRowMapper(RowMapper<T> rowMapper){
            checkArgument(rowMapper != null, "rowMapper can not be null");
            return toBuilder().setRowMapper(rowMapper).build();
        }

        public Read<T> withCoder(Coder<T> coder){
            checkArgument(coder != null, "coder can not be null");
            return toBuilder().setCoder(coder).build();
        }

        @Override
        public PCollection<T> expand(PBegin input) {
            checkArgument(getQuery() != null, "withQuery() is required");
            checkArgument(getRowMapper() != null, "withRowMapper() is required");
            checkArgument(getCoder() != null, "withCoder() is required");
            checkArgument(
                    (getDriverConfiguration() != null), "withDriverConfiguration() is required");

            return input.apply(Create.of((Void) null)).apply(Neo4jIO.<Void, T>readAll()
                    .withDriverConfiguration(getDriverConfiguration())
                    .withQuery(getQuery())
                    .withCoder(getCoder())
                    .withRowMapper(getRowMapper()));
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.add(DisplayData.item("query", getQuery()));
            builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
            builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
            getDriverConfiguration().populateDisplayData(builder);
        }
    }

    @AutoValue
    public abstract static class ReadAll<ParameterT, OutputT> extends  PTransform<PCollection<ParameterT>, PCollection<OutputT>>{

        @Nullable
        abstract DriverConfiguration getDriverConfiguration();
        @Nullable
        abstract ValueProvider<String> getQuery();
        @Nullable
        abstract Coder<OutputT> getCoder();

        @Nullable
        abstract RowMapper<OutputT> getRowMapper();

        abstract Builder<ParameterT, OutputT> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<ParameterT, OutputT> {
            abstract Builder<ParameterT, OutputT> setDriverConfiguration(
                    DriverConfiguration config);

            abstract Builder<ParameterT, OutputT> setQuery(ValueProvider<String> query);

            abstract Builder<ParameterT, OutputT> setRowMapper(RowMapper<OutputT> rowMapper);

            abstract Builder<ParameterT, OutputT> setCoder(Coder<OutputT> coder);

            abstract ReadAll<ParameterT, OutputT> build();
        }

        public ReadAll<ParameterT, OutputT>  withDriverConfiguration(DriverConfiguration config){
            checkArgument(config != null, "driverConfiguration can not be null");
            checkArgument(config instanceof Serializable, "driverConfiguration must be Serializable");
            return toBuilder().setDriverConfiguration(config).build();
        }

        public ReadAll<ParameterT, OutputT> withQuery(String query){
            checkArgument(query != null, "query can not be null");
            return withQuery(ValueProvider.StaticValueProvider.of(query));
        }

        public ReadAll<ParameterT, OutputT> withQuery(ValueProvider<String> query){
            checkArgument(query != null, "query can not be null");
            return toBuilder().setQuery(query).build();
        }

        public ReadAll<ParameterT, OutputT> withRowMapper(RowMapper<OutputT> rowMapper){
            checkArgument(rowMapper != null, "rowMapper can not be null");
            return toBuilder().setRowMapper(rowMapper).build();
        }

        public ReadAll<ParameterT, OutputT> withCoder(Coder<OutputT> coder){
            checkArgument(coder != null, "coder can not be null");
            return toBuilder().setCoder(coder).build();
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.add(DisplayData.item("query", getQuery()));
            builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
            builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
            getDriverConfiguration().populateDisplayData(builder);
        }

        @Override
        public PCollection<OutputT> expand(PCollection<ParameterT> input) {

            return input.apply(ParDo.of(new ReadFn<>(getDriverConfiguration(),
                    getQuery(),
                    getRowMapper())))
                    .setCoder(getCoder())
                    .apply(new Reparallelize<>());
        }
    }


    private static class ReadFn<ParameterT,OutputT> extends DoFn<ParameterT, OutputT>{
        private final DriverConfiguration driverConfiguration;
        private final ValueProvider<String> query;
        private final RowMapper<OutputT> rowMapper;
        private Driver driver;
        private Session session;

        public ReadFn(DriverConfiguration driverConfiguration, ValueProvider<String> query, RowMapper<OutputT> rowMapper) {
            this.driverConfiguration = driverConfiguration;
            this.query = query;
            this.rowMapper = rowMapper;
        }

        @Setup
        public void setup() throws Exception{
            driver = driverConfiguration.buildDriver();

            session = driver.session();

        }
        @ProcessElement
        public void processElement(ProcessContext context) throws Exception {
            try (Transaction tx = session.beginTransaction()) {
                try {

                    StatementResult result = tx.run(query.get());

                    while (result.hasNext()){
                        context.output(rowMapper.mapRow(result));
                    }
                    tx.success();
                }catch (Exception e){
                    LOG.warn("Deadlock detected, retrying", e);
                    tx.failure();

                }
            }
        }

        @Teardown
        public void teardown() throws Exception {
            if (driver instanceof AutoCloseable) {
                ((AutoCloseable) driver).close();
            }
        }
    }


    @AutoValue
    public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

        @Nullable
        abstract  ValueProvider<String> getStatement();

        abstract  long getBatchSize();

        @Nullable
        abstract  ValueProvider<String> getOptionsType();

        @Nullable
        abstract  ValueProvider<String> getLabel();

        abstract Builder<T> toBuilder();
        @Nullable
        abstract DriverConfiguration getDriverConfiguration();

        @AutoValue.Builder
        abstract static class Builder<T>{
            abstract Builder<T> setDriverConfiguration(DriverConfiguration config);
            abstract Builder<T> setStatement(ValueProvider<String> statement);
            abstract Builder<T> setBatchSize(long batchSize);
            abstract Builder<T> setOptionsType(ValueProvider<String> optionsType);
            abstract Builder<T> setLabel(ValueProvider<String> label);
            abstract Write<T> build();
        }

        public Write<T> withLabel(String label){
            checkArgument(label != null, "label can not be null");
            return withLabel(ValueProvider.StaticValueProvider.of(label));
        }

        public Write<T> withLabel(ValueProvider<String> label){
            checkArgument(label != null, "label can not be null");
            return toBuilder().setLabel(label).build();
        }

        public Write<T> withOptionsType(String optionsType){
            checkArgument(optionsType != null, "optionsType can not be null");
            return withOptionsType(ValueProvider.StaticValueProvider.of(optionsType));
        }

        public Write<T> withOptionsType(ValueProvider<String> optionsType){
            checkArgument(optionsType != null, "optionsType can not be null");
            return toBuilder().setOptionsType(optionsType).build();
        }

        public Write<T> withDriverConfiguration(DriverConfiguration config){
            checkArgument(config != null, "driverConfiguration can not be null");
            checkArgument(config instanceof Serializable, "driverConfiguration must be Serializable");
            return toBuilder().setDriverConfiguration(config).build();
        }

        public Write<T> withStatement(ValueProvider<String> statement){
            checkArgument(statement != null, "optionsType can not be null");
            return toBuilder().setStatement(statement).build();
        }

        public Write<T> withStatement(String statement){
            checkArgument(statement != null, "optionsType can not be null");
            return withStatement(ValueProvider.StaticValueProvider.of(statement));
        }
        public Write<T> withBatchSize(long batchSize){
            checkArgument(batchSize > 0, "batchSize size must be > 0, but was %s", batchSize);
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
            String optionsType = spec.getOptionsType().get().toString();
            String statement = spec.getStatement().get().toString();
            String result=statement;
            if(BaseUtil.isNotBlank(optionsType) && BaseUtil.isNotBlank(statement)){
                if(optionsType.equals(Neo4jEnum.RELATE.getName())){
                    Value value1 = value.get(spec.getLabel().get().toString());
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



    private static class Reparallelize<T> extends PTransform<PCollection<T>, PCollection<T>> {
        @Override
        public PCollection<T> expand(PCollection<T> input) {
            // See https://issues.apache.org/jira/browse/BEAM-2803
            // We use a combined approach to "break fusion" here:
            // (see https://cloud.google.com/dataflow/service/dataflow-service-desc#preventing-fusion)
            // 1) force the data to be materialized by passing it as a side input to an identity fn,
            // then 2) reshuffle it with a random key. Initial materialization provides some parallelism
            // and ensures that data to be shuffled can be generated in parallel, while reshuffling
            // provides perfect parallelism.
            // In most cases where a "fusion break" is needed, a simple reshuffle would be sufficient.
            // The current approach is necessary only to support the particular case of JdbcIO where
            // a single query may produce many gigabytes of query results.
            PCollectionView<Iterable<T>> empty =
                    input
                            .apply("Consume", Filter.by(SerializableFunctions.constant(false)))
                            .apply(View.asIterable());
            PCollection<T> materialized =
                    input.apply(
                            "Identity",
                            ParDo.of(
                                    new DoFn<T, T>() {
                                        @ProcessElement
                                        public void process(ProcessContext c) {
                                            c.output(c.element());
                                        }
                                    })
                                    .withSideInputs(empty));
            return materialized.apply(Reshuffle.viaRandomKey());
        }
    }
}
