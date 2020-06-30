package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.common.constantenum.Neo4jEnum;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jObject;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jCode;
import com.knowlegene.parent.process.transform.TypeConversion;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/9/23 17:46
 */
public class Neo4jImportJob extends ImportJobBase{
    private final String label="type";

    public Neo4jImportJob() {
    }

    public Neo4jImportJob(SwapOptions opts) {
        super(opts);
    }


    /**
     * cypher 语句
     * @param rows
     */
    private void cypherSave(PCollection<Row> rows){
        List<String> fieldNames = rows.getSchema().getFieldNames();
        if(BaseUtil.isBlankSet(fieldNames)){
            getLogger().error("fieldNames is null");
            return ;
        }
        String cypher = options.getCypher();
        PCollection<Neo4jObject> apply = rows.apply(ParDo.of(new TypeConversion.RowAndNeo4jObject(label,fieldNames)));
        saveNeo4jObject(apply,cypher, Neo4jEnum.SAVE.getValue());
    }

    /**
     * 模板
     * @param rows
     */
    private void formatSave(PCollection<Row> rows){
        Neo4jCode neo4jCode = new Neo4jCode();
        neo4jCode.toDSL(options.getNeoFormat());
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

        PCollection<Neo4jObject> apply = rows.apply(ParDo.of(new TypeConversion.RowAndNeo4jObject(label,type, keys)));
        saveNeo4jObject(apply,cypher,type);
    }

    private void saveNeo4jObject(PCollection<Neo4jObject> neo4jObjects,String dsl,int optionsType){
//        if(neo4jObjects == null){
//            getLogger().error("neo4jObjects is null");
//            return;
//        }
//        if(BaseUtil.isBlank(dsl)){
//            getLogger().error("dsl is null");
//            return;
//        }
//        if(optionsType == Neo4jEnum.SAVE.getValue()){
//            neo4jObjects.apply(this.getNeo4jSwap().write(dsl));
//        }else if(optionsType == Neo4jEnum.RELATE.getValue()){
//            neo4jObjects.apply(this.getNeo4jSwap().relate(dsl,label));
//        }

    }




    public static void save(PCollection<Row> rows) {
//        if(rows != null){
//            String cypher = options.getCypher();
//            String neoFormat = options.getNeoFormat();
//            if (BaseUtil.isNotBlank(cypher)) {
//                cypherSave(rows);
//            }else if(BaseUtil.isNotBlank(neoFormat)){
//                formatSave(rows);
//            }else{
//                getLogger().error("cypher is null");
//            }
//        }else{
//            getLogger().error("rows is null");
//        }
    }




}
