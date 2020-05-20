--企业基本信息表
create table IF NOT EXISTS b_mart.B_C01_BASE_INF_FS ( DATA_DATE string,ENTID string,ENTNAME string,UNISCID string,REGNO string,LICID string,ENTTYPE string,ENTTYPE_CN string,INDUSTRYPHY string,INDUSTRYCO string,OPFROM DATE,OPTO DATE,POSTALCODE string,TEL string,EMAIL string,ESDATE DATE,APPRDATE DATE,REGORG string,REGORG_CN string,REGCAP string,FN_REG_CAP_RES string,ENTSTATUS string,ENTSTATUS_CN string,OPSCOPE string,DOMDISTRICT string,DOM string,EMPNUM int,SCONFORM string,SCONFORM_CN string,REGCAPCUR string,REGCAPCUR_CN string,COUNTRY string,COUNTRY_CN string,LEREPNAME string,PERSONID string,CANDATE DATE,CANREASON string,REVDATE DATE,REVREASON string,ALTDATE DATE,INV_NUMS int,PERSON_NUMS int,industry_desc string) PARTITIONED BY (NODENUM string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
create table IF NOT EXISTS a_mart.A_C01_BASE_INF_FS ( DATA_DATE string,ENTID string,ENTNAME string,UNISCID string,REGNO string,LICID string,ENTTYPE string,ENTTYPE_CN string,INDUSTRYPHY string,INDUSTRYCO string,OPFROM DATE,OPTO DATE,POSTALCODE string,TEL string,EMAIL string,ESDATE DATE,APPRDATE DATE,REGORG string,REGORG_CN string,REGCAP string,FN_REG_CAP_RES string,ENTSTATUS string,ENTSTATUS_CN string,OPSCOPE string,DOMDISTRICT string,DOM string,EMPNUM int,SCONFORM string,SCONFORM_CN string,REGCAPCUR string,REGCAPCUR_CN string,COUNTRY string,COUNTRY_CN string,LEREPNAME string,PERSONID string,CANDATE DATE,CANREASON string,REVDATE DATE,REVREASON string,ALTDATE DATE,INV_NUMS int,PERSON_NUMS int,industry_desc string) PARTITIONED BY (NODENUM string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--常用参数编码表
create table IF NOT EXISTS b_mart.B_A01_CODE_LIB_CR (DATA_DATE String,ITM_SOURCE String,ITM_NO String,ITM_NAME String,PA_CODE String,PA_ID String,PA_NAME String,PA_LEVEL String,PA_CODSUP String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
create table IF NOT EXISTS a_mart.A_A01_CODE_LIB_CR (unique_id String,DATA_DATE String,ITM_SOURCE String,ITM_NO String,ITM_NAME String,PA_CODE String,PA_ID String,PA_NAME String,PA_LEVEL String,PA_CODSUP String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--工商企业实体曾用名表
create table IF NOT EXISTS b_mart.B_C02_USD_NAME_FS (DATA_DATE String,ENTID String,ENTNAME String,USEDNAME String) PARTITIONED BY (NODENUM String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
create table IF NOT EXISTS a_mart.A_C02_USD_NAME_FS (unique_id String,DATA_DATE String,ENTID String,ENTNAME String,USEDNAME String) PARTITIONED BY (NODENUM String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--法人企业主要管理人员信息
create table IF NOT EXISTS b_mart.B_C05_PERSON_INF_FS (DATA_DATE String,ENTID String,PERSONID String,PERNAME String,CERTYPE String,CERNO String,TEL String,COUNTRY String,LEREPSIGN String,POSITION String,POSITION_CN String) PARTITIONED BY (NODENUM String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
create table IF NOT EXISTS a_mart.A_C05_PERSON_INF_FS (unique_id String,DATA_DATE String,ENTID String,PERSONID String,PERNAME String,CERTYPE String,CERNO String,TEL String,COUNTRY String,LEREPSIGN String,POSITION String,POSITION_CN String) PARTITIONED BY (NODENUM String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--法人企业变更公告信息
create table IF NOT EXISTS b_mart.B_C07_CHG_INF_FS (DATA_DATE String,ENTID String,ALTITEM String,ALTITEM_CN String,ALTBE String,ALTAF String,ALTDATE DATE) PARTITIONED BY (NODENUM String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
create table IF NOT EXISTS a_mart.A_C07_CHG_INF_FS (unique_id String,DATA_DATE String,ENTID String,ALTITEM String,ALTITEM_CN String,ALTBE String,ALTAF String,ALTDATE DATE) PARTITIONED BY (NODENUM String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--法人企业注销信息
create table IF NOT EXISTS b_mart.B_C08_CANCEL_FS (DATA_DATE String,ENTID String,CANDATE DATE,CANREASON String) PARTITIONED BY (NODENUM String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--法人企业吊销信息
create table IF NOT EXISTS b_mart.B_C09_REVOKE_FS (DATA_DATE String,ENTID String,REVDATE DATE,REVREASON String) PARTITIONED BY (NODENUM String) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--投资担保机构实体信息表
create table IF NOT EXISTS b_mart.B_C10_INV_GRT_AGC_CR (name varchar(100),type varchar(10),source varchar(10)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--企业分支机构表
create table IF NOT EXISTS b_mart.B_C10_BRANCH_FS (DATA_DATE VARCHAR(10),ENTID VARCHAR(50),ENTNAME VARCHAR(500),BRENTID VARCHAR(50),BRENTNAME VARCHAR(200),BRUNISCID VARCHAR(200),BRREGNO VARCHAR(500),BRREGORG_CN VARCHAR(2000),ISVALID VARCHAR(1),JOBID VARCHAR(10)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'STORED AS TEXTFILE;
create table IF NOT EXISTS a_mart.A_C10_BRANCH_FS
(unique_id String,DATA_DATE VARCHAR(10),ENTID VARCHAR(50),ENTNAME VARCHAR(500),
BRENTID VARCHAR(50),BRENTNAME VARCHAR(200),BRUNISCID VARCHAR(200),
BRREGNO VARCHAR(500),BRREGORG_CN VARCHAR(2000),ISVALID VARCHAR(1),JOBID VARCHAR(10)
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' STORED AS TEXTFILE;
--组织机构基本信息
create table IF NOT EXISTS b_mart.B_C11_ORG_BASIC_INF_FS(data_date varchar(10),entid varchar(50),uniscid varchar(20),orgcode varchar(20),orgcerno varchar(50),regioncode varchar(10),orgname string,orgtype varchar(10),ischaritable varchar(2),issauth varchar(20),issauthname varchar(100),regdate varchar(20),chargorg varchar(20),chargname string,orgstatus varchar(2),orgfrom varchar(20),orgto varchar(20),bizaddr string,bizscope string,tel varchar(50),fax varchar(30),email varchar(50),website varchar(100),regcap decimal(24,0),regcapunit varchar(10),regcapcurr varchar(10),lerepname varchar(100),jobid varchar(20))ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
create table IF NOT EXISTS a_mart.A_C11_ORG_BASIC_INF_FS(data_date varchar(10),entid varchar(50),uniscid varchar(20),orgcode varchar(20),orgcerno varchar(50),regioncode varchar(10),orgname string,orgtype varchar(10),ischaritable varchar(2),issauth varchar(20),issauthname varchar(100),regdate varchar(20),chargorg varchar(20),chargname string,orgstatus varchar(2),orgfrom varchar(20),orgto varchar(20),bizaddr string,bizscope string,tel varchar(50),fax varchar(30),email varchar(50),website varchar(100),regcap decimal(24,0),regcapunit varchar(10),regcapcurr varchar(10),lerepname varchar(100),jobid varchar(20))ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
--MD5补全invid 为空的企业股东表
create table b_mart.D_COMP_INVINF_MD5 like ods_mart.D_COMP_INVINF;
--MD5补全personid 为空的企业高管表
create table b_mart.D_COMP_PERSONINF_MD5 like ODS_MART.D_COMP_PERSONINF;
