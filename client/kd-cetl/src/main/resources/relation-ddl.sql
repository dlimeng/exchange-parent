--法人企业股东信息
create table IF NOT EXISTS b_mart.B_C03_INV_INF_FS (DATA_DATE String,ENTID String,GDID String,INVID String,INVNAME String,INVTYPE String,INVTYPE_CN String,BLICTYPE String,BLICTYPE_CN String,BLICNO String,ISCP String,COUNTRY String,COUNTRY_CN String,CURRENCY String,SUBCONAM DECIMAL(20,6),SUBCONDATE DATE,ACCCONAM DECIMAL(20,6),BALDELPER DATE,SCONFORM String,SCONFORM_CN String,RESPFORM_CN String,DETAILCHECK String) PARTITIONED BY (NODENUM String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--法人企业股东信息
create table IF NOT EXISTS a_mart.A_C03_INV_INF_FS (DATA_DATE String,ENTID String,GDID String,INVID String,INVNAME String,INVTYPE String,INVTYPE_CN String,BLICTYPE String,BLICTYPE_CN String,BLICNO String,ISCP String,COUNTRY String,COUNTRY_CN String,CURRENCY String,SUBCONAM DECIMAL(20,6),SUBCONDATE DATE,ACCCONAM DECIMAL(20,6),BALDELPER DATE,SCONFORM String,SCONFORM_CN String,RESPFORM_CN String,DETAILCHECK String) PARTITIONED BY (NODENUM String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--法人企业投资关系表
create table IF NOT EXISTS b_mart.B_D01_INV_REL_CR (ENTID String,INVID String,INVNAME String,ISCP String,CURRENCY String,SUBCONAM DECIMAL(20,6),TOTALREGCAP DECIMAL(20,6),FN_INV_PROP DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--企业关联关系表
create table IF NOT EXISTS e_mart.E_D07_BUS_REL_CR
(FS_Id string,FS_From_BUS_Id string,FS_From_BUS_NM string,FS_To_BUS_Id string,
FS_To_BUS_Nm string,FN_Val_Wt double,FS_IS_PERSON String,FD_Crt_Tm date ,
FD_Upd_Tm date ,FN_Backup_1 int,FN_Backup_2 decimal(18,2),FS_Backup_3 string,
FS_Backup_4 string)
PARTITIONED BY (FS_Rel_Typ String)
ROW FORMAT DELIMITED FIELDS
TERMINATED BY '\t' STORED AS TEXTFILE
location '/user/root/20190430/e_07_bus_rel_cr_all';


LOAD DATA INPATH '/user/root/20190430/e_07_bus_rel_cr_all' OVERWRITE INTO TABLE e_mart.E_D07_BUS_REL_CR;

--企业注册资本统计表
create table IF NOT EXISTS e_mart.E_C01_BASE_INF_INCREA_FS ( ENTID string,TOTALREGCAP string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--创建企业最终注册资本信息表
CREATE TABLE IF NOT EXISTS e_mart.E_C01_BASE_INF_INCREA_FS(
ENTID VARCHAR(50),
TOTALREGCAP DECIMAL(20,6),
ACCCONAM DECIMAL(20,6)
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

--企业关系节点中间表
create table IF NOT EXISTS e_mart.E_D07_BUS_NODE_MID_CR (FS_Id varchar(100),ENTID varchar(50),ENTNAME varchar(600),REGCAP varchar(20),ISCP varchar(10),entstatus varchar(10)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--企业关系节点加工中间表
create table IF NOT EXISTS e_mart.E_D07_BUS_NODE_CR_bak (FS_Id varchar(100),ENTID varchar(50),ENTNAME varchar(600),REGCAP varchar(20),ISCP varchar(10),inv_grt_typ VARCHAR(10),entstatus varchar(10)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--企业关系节点表
create table IF NOT EXISTS e_mart.E_D07_BUS_NODE_CR (FS_Id varchar(100),ENTID varchar(50),ENTNAME varchar(600),REGCAP varchar(20),ISCP varchar(10),inv_grt_typ VARCHAR(10),entstatus varchar(10)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

--创建企业关系临时表
create table IF NOT EXISTS e_mart.E_D07_BUS_REL_CR_bak like e_mart.E_D07_BUS_REL_CR;

create external table movie_table
(
movieId STRING,
title STRING,
genres STRING
)
row format delimited fields terminated by ','
stored as textfile
location '/hive_operate/movie_table';
