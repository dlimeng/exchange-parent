--企业实体表
create table IF NOT EXISTS a_mart.A_C02_BUS_ENT_CR (ENTID VARCHAR(50),ENTNAME VARCHAR(600),nodenum varchar(10)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--非注册企业实体表
create table IF NOT EXISTS a_mart.A_C02_FOREIGN_BUS_ENT_CR LIKE a_mart.A_C02_BUS_ENT_CR;
--个人实体表
create table IF NOT EXISTS a_mart.A_C06_PERSON_ENT_CR (PERSONID VARCHAR(50),PERNAME VARCHAR(200),nodenum varchar(10)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--个人实体中间表
create table IF NOT EXISTS a_mart.A_C06_PERSON_ENT_CR_bak like a_mart.A_C06_PERSON_ENT_CR;
--正则类自然人实体
create table  if not exists a_mart.A_C06_PERSON_ENT_CR_regex like a_mart.A_C06_PERSON_ENT_CR;
--组织机构实体信息
create table IF NOT EXISTS a_mart.A_C11_ORG_BASIC_ENT_FS(entid varchar(50),orgname VARCHAR(600))ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--正则类组织机构实体
create table if not exists a_mart.A_C11_ORG_BASIC_ENT_FS_regex like a_mart.A_C11_ORG_BASIC_ENT_FS;
--待正则匹配实体表
create table if not exists a_mart.A_ENT_ALL_REGEX like a_mart.A_C02_BUS_ENT_CR;

--全量的企业表
create table  a_mart.A_BUS_ORG_ALL like a_mart.a_C02_BUS_ENT_CR;