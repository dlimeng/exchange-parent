package com.knowlegene.parent.process.transform;

import com.knowlegene.parent.config.common.constantenum.DatabaseTypeEnum;
import com.knowlegene.parent.config.common.constantenum.HiveTypeEnum;
import com.knowlegene.parent.config.deploy.HiveConfig;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.runners.options.IndexerPipelineOptions;
import com.knowlegene.parent.config.util.BaseSqlParserFactoryUtil;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.config.util.JdbcUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.sql.DataSource;
import java.io.Serializable;
import java.util.Map;

/**
 * 自定义函数
 * @Author: limeng
 * @Date: 2019/7/24 19:05
 */
public class HiveDb implements Serializable {

    private static final long serialVersionUID = 1664820362997774072L;

    /**
     * 基准正则验证非空
     */
    public static class DatumRexFn<T> extends DoFn<String, String> {
        private static final long serialVersionUID = 7653454396353714463L;
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            String element = c.element();
            String notBlankRex = BaseUtil.isNotBlankRex(element);
            if(BaseUtil.isNotBlank(notBlankRex)){
                c.output(notBlankRex);
            }
        }
    }

    /**
     * 处理链路
     * @param <T>
     */
    public static class DoFnHiveLink<T> extends DoFn<String, String> {
        private static final long serialVersionUID = 4482669553996421592L;
        private Logger logger = LoggerFactory.getLogger(DoFnHiveLink.class);

        HiveTypeEnum select1 = HiveTypeEnum.SELECT1;
        HiveTypeEnum save1 = HiveTypeEnum.SAVE1;
        HiveTypeEnum load1 = HiveTypeEnum.LOAD1;
        int value = DatabaseTypeEnum.HIVE.getValue();
        Row build= JdbcUtil.getInitWrite().getValue();
        Pipeline pipeline = null;
        /**
         * 获取源
         * @return
         */
        private DataSource getDataSource(){
            return new HiveConfig().getHiveDataSource1();
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            String sql = c.element();
            //初始化
            if(pipeline == null){
                IndexerPipelineOptions pipelineOptions = (IndexerPipelineOptions)c.getPipelineOptions();
                pipeline = Pipeline.create(pipelineOptions);
            }

            int status = BaseSqlParserFactoryUtil.generateParser(value, sql);
            if(status == select1.getValue()){
                //导流
                c.output(sql);
            }else{
                processElement(pipeline,sql,status);
            }


            pipeline.run().waitUntilFinish();
         }

        /**
         * 处理源操作链路
         * @param pipeline
         * @param sql
         * @param status
         * @throws Exception
         */
        private void processElement(Pipeline pipeline,String sql,int status) throws Exception {
            if(BaseUtil.isBlank(sql)){
                return;
            }
            //sql类型操作
            if(status == save1.getValue() || status == load1.getValue()){
                pipeline.apply(Create.of(build)).apply(JdbcUtil.writeHive(getDataSource(),sql));
                logger.info("save=>sql:{}",sql);
            }else if(status != select1.getValue()) {

                int i = JdbcUtil.create(getDataSource(),sql);
                logger.info("create=>sql:{},affected:{}",sql,i);
            }
        }
    }

    /**
     * 处理链路保存
     * save
     * @param <T>
     */
    public static class DoFnHiveSaveLink<T> extends DoFn<String, String> {

        private static final long serialVersionUID = 4901349577623349383L;
        private Logger logger = LoggerFactory.getLogger(DoFnHiveSaveLink.class);
        HiveTypeEnum save1 = HiveTypeEnum.SAVE1;
        HiveTypeEnum load1 = HiveTypeEnum.LOAD1;
        HiveTypeEnum create1 = HiveTypeEnum.CREATE1;
        HiveTypeEnum select1 = HiveTypeEnum.SELECT1;
        int value = DatabaseTypeEnum.HIVE.getValue();
        Row build=JdbcUtil.getInitWrite().getValue();
        Pipeline pipeline = null;

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            String sql = c.element();

            //初始化
            if(pipeline == null){
                PipelineOptions pipelineOptions = c.getPipelineOptions();
                pipeline = Pipeline.create(pipelineOptions);
            }
            int status = BaseSqlParserFactoryUtil.generateParser(value, sql);
            if(status != create1.getValue() || status != select1.getValue()){
                pipeline.apply(Create.of(build)).apply(JdbcUtil.writeHive(getDataSource(), sql));
                logger.info("save=>sql:{}",sql);
            }else{
                //导流
                c.output(sql);
            }
            pipeline.run();
        }


        public DoFnHiveSaveLink() {

        }

        private DataSource getDataSource(){
            return  new HiveConfig().getHiveDataSource1();
        }
    }

    /**
     * 处理链路定义语句
     * ddl
     * @param <T>
     */
    public static class DoFnHiveDdlLink<T> extends DoFn<String, String> implements Serializable{
        private static final long serialVersionUID = -5858962612326079475L;
        private Logger logger = LoggerFactory.getLogger(DoFnHiveDdlLink.class);

        HiveTypeEnum select1 = HiveTypeEnum.SELECT1;
        HiveTypeEnum save1 = HiveTypeEnum.SAVE1;
        HiveTypeEnum load1 = HiveTypeEnum.LOAD1;
        int value = DatabaseTypeEnum.HIVE.getValue();
        Row build=JdbcUtil.getInitWrite().getValue();
        Pipeline pipeline = null;
        /**
         * 获取源
         * @return
         */
        private DataSource getDataSource(){
            return new HiveConfig().getHiveDataSource1();
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            String sql = c.element();
            //初始化
            if(pipeline == null){
                PipelineOptions pipelineOptions = c.getPipelineOptions();
                pipeline = Pipeline.create(pipelineOptions);
            }
            int status = BaseSqlParserFactoryUtil.generateParser(value, sql);
            if(status != select1.getValue() && status!= save1.getValue() &&  status!= load1.getValue()) {
                pipeline.apply(Create.of(build)).apply(JdbcUtil.writeHive(getDataSource(), sql));
                logger.info("create=>sql:{}",sql);
            }else{
                //导流
                c.output(sql);
            }
            pipeline.run();
        }
    }


    /**
     * hive查询
     * @param sql sql
     * @return
     * @throws Exception
     */
    public static JdbcIO.Read<Map<String, ObjectCoder>> selectByHive(DataSource dataSource,String sql) throws Exception {
        if(dataSource!= null){
            return JdbcIO.<Map<String,ObjectCoder>>read().
                    withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                            .create(dataSource)

                    ).withCoder(MapCoder.of(StringUtf8Coder.of(), SerializableCoder.of(ObjectCoder.class)))
                    .withQuery(sql).withFetchSize(10000)
                    .withRowMapper(new JdbcTransform.MapHiveRowMapper());
        }
        return null;
    }

    /**
     * 批量保存 hive
     * @param sql
     * @return
     */
    public static JdbcIO.Write<Row> batchHiveSave(DataSource dataSource,String sql){
        if(dataSource!= null){
            return JdbcIO.<Row>write()
                    .withDataSourceConfiguration(
                            JdbcIO.DataSourceConfiguration.create(
                                    dataSource).withConnectionProperties("hive"))
                    .withStatement(sql)
                    .withPreparedStatementSetter(new JdbcTransform.PrepareStatementFromHiveRow());
        }
        return null;
    }
}
