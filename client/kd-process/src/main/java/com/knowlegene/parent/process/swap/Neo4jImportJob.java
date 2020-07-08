package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.event.Neo4jImportType;
import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.common.constantenum.Neo4jEnum;
import com.knowlegene.parent.process.pojo.ObjectCoder;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jObject;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jCode;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jOptions;
import com.knowlegene.parent.process.swap.event.Neo4jImportTaskEvent;
import com.knowlegene.parent.process.transform.TypeConversion;
import com.knowlegene.parent.scheduler.event.EventHandler;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;
import java.util.Map;

/**
 * @Author: limeng
 * @Date: 2019/9/23 17:46
 */
public class Neo4jImportJob extends ImportJobBase{
    private static Neo4jOptions neo4jOptions;

    private static final String label="type";

    public Neo4jImportJob() {
    }

    public Neo4jImportJob(SwapOptions opts) {
        super(opts);
    }

    private static Neo4jOptions getDbOptions(){
        if(neo4jOptions == null){
            String name = DBOperationEnum.NEO4J_IMPORT.getName();
            Object options = getOptions(name);
            if(options != null){
                neo4jOptions = (Neo4jOptions)options;
            }
        }
        return neo4jOptions;
    }

    /**
     * cypher 语句
     * @param rows
     */
    private static void cypherSave(PCollection<Map<String, ObjectCoder>> rows){
        List<String> fieldNames = rows.getSchema().getFieldNames();
        if(BaseUtil.isBlankSet(fieldNames)){
            getLogger().error("fieldNames is null");
            return ;
        }
        String cypher = getDbOptions().getCypher();
        PCollection<Neo4jObject> apply = rows.apply(ParDo.of(new TypeConversion.MapAndNeo4jObject(label,fieldNames)));
        saveNeo4jObject(apply,cypher, Neo4jEnum.SAVE.getValue());
    }

    /**
     * 模板
     * @param rows
     */
    private static void formatSave(PCollection<Map<String, ObjectCoder>> rows){
        Neo4jCode neo4jCode = new Neo4jCode();
        neo4jCode.toDSL(getDbOptions().getNeoFormat());
        Integer type = neo4jCode.getType();
        List<String> keys = neo4jCode.getKeys();
        if(type == null){
            getLogger().error("format type is null");
            return ;
        }
        if(BaseUtil.isBlankSet(keys)){
            getLogger().error("keys is null");
            return ;
        }
        String cypher = neo4jCode.getDsl();
        if(BaseUtil.isBlank(cypher)){
            getLogger().error("cypher is null");
            return ;
        }

        PCollection<Neo4jObject> apply = rows.apply(ParDo.of(new TypeConversion.MapAndNeo4jObject(label,type, keys)));
        saveNeo4jObject(apply,cypher,type);
    }

    private static void saveNeo4jObject(PCollection<Neo4jObject> neo4jObjects,String dsl,int optionsType){
        if(neo4jObjects == null){
            getLogger().error("neo4jObjects is null");
            return;
        }
        if(BaseUtil.isBlank(dsl)){
            getLogger().error("dsl is null");
            return;
        }
        if(optionsType == Neo4jEnum.SAVE.getValue()){
            neo4jObjects.apply(getNeo4jSwapImport().write(dsl));
        }else if(optionsType == Neo4jEnum.RELATE.getValue()){
            neo4jObjects.apply(getNeo4jSwapImport().relate(dsl,label));
        }

    }



    public static void save(PCollection<Map<String, ObjectCoder>> rows) {
        if(rows != null){
            String cypher = getDbOptions().getCypher();
            String neoFormat = getDbOptions().getNeoFormat();
            if (BaseUtil.isNotBlank(cypher)) {
                cypherSave(rows);
            }else if(BaseUtil.isNotBlank(neoFormat)){
                formatSave(rows);
            }else{
                getLogger().error("cypher is null");
            }
        }else{
            getLogger().error("rows is null");
        }
    }

    public static class Neo4jImportDispatcher implements EventHandler<Neo4jImportTaskEvent> {
        @Override
        public void handle(Neo4jImportTaskEvent event) {
            if(event.getType() == Neo4jImportType.T_IMPORT){
                getLogger().info("Neo4jImportDispatcher is start");

                if(CacheManager.isExist(DBOperationEnum.PCOLLECTION_QUERYS.getName())){
                    save((PCollection<Map<String, ObjectCoder>>)CacheManager.getCache(DBOperationEnum.PCOLLECTION_QUERYS.getName()));
                }

            }
        }
    }



}
