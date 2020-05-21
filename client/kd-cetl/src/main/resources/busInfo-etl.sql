set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set hive.execution.engine=tez;
set hive.tez.container.size=4096;



truncate table b_mart.B_C08_CANCEL_FS;

insert into b_mart.B_C08_CANCEL_FS partition (NODENUM) select DATA_DATE ,ENTID ,CANDATE ,CANREASON ,NODENUM from ODS_MART.D_COMP_CANCEL ;

truncate table b_mart.B_C09_REVOKE_FS;

insert into b_mart.B_C09_REVOKE_FS partition (NODENUM) select DATA_DATE ,ENTID ,REVDATE ,REVREASON ,NODENUM from ODS_MART.D_COMP_REVOKE ;

truncate table b_mart.B_C10_BRANCH_FS;

insert into  b_mart.B_C10_BRANCH_FS select * from ods_mart.D_COMP_BRANCH;

truncate table a_mart.A_C10_BRANCH_FS;

insert into  a_mart.A_C10_BRANCH_FS select regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", ""),t.* from b_mart.B_C10_BRANCH_FS t;

truncate table b_mart.B_C07_CHG_INF_FS;

insert into b_mart.B_C07_CHG_INF_FS partition (NODENUM) select  DATA_DATE ,ENTID ,ALTITEM ,ALTITEM_CN ,ALTBE ,ALTAF ,to_date(ALTDATE) as ALTDATE ,NODENUM from ODS_MART.D_COMP_CHGINF ;

truncate table a_mart.A_C07_CHG_INF_FS;

insert into a_mart.A_C07_CHG_INF_FS partition (NODENUM) select regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", ""),t.* from b_mart.B_C07_CHG_INF_FS t ;

truncate table b_mart.B_C02_USD_NAME_FS;

insert into b_mart.B_C02_USD_NAME_FS partition (NODENUM) select DATA_DATE,ENTID,ENTNAME,USEDNAME,NODENUM from ods_mart.D_COMP_USEDNAME;

truncate table a_mart.A_C02_USD_NAME_FS;

insert into a_mart.A_C02_USD_NAME_FS partition (NODENUM) select regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", ""),DATA_DATE,ENTID,ENTNAME,USEDNAME,NODENUM from b_mart.B_C02_USD_NAME_FS;

