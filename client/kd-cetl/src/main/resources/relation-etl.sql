set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set hive.execution.engine=tez;
set hive.tez.container.size=4096;



truncate table b_mart.D_COMP_INVINF_MD5;

insert into b_mart.D_COMP_INVINF_MD5 select data_date,nodenum,entid,gdid,invid ,regexp_replace(NVL(trim(INVNAME),''),'[\\s＊〓\\\"]|[\\u0000-\\u001f]','') as INVNAME,invtype,invtype_cn,blictype,blictype_cn,blicno,country,country_cn,currency,subconam,subcondate,accconam,baldelper,iscp,detailcheck,sconform,sconform_cn,respform_cn,jobid,domid from ods_mart.D_COMP_INVINF where length(trim(invname)) > 1 and lower(trim(invname)) != 'null';

truncate table b_mart.B_C03_INV_INF_FS ;
--法人企业股东信息表关联个人实体表，并标记投资关系为个人
insert into b_mart.B_C03_INV_INF_FS partition (NODENUM)
select DATA_DATE,a.ENTID,GDID,case when a.INVID is  null then b.entid else a.INVID end as invid,INVNAME,INVTYPE,INVTYPE_CN,
BLICTYPE,BLICTYPE_CN,BLICNO,case when b.entname is not null then 'C' else 'P'  end  as iscp,COUNTRY,COUNTRY_CN,CURRENCY,SUBCONAM,
SUBCONDATE,ACCCONAM,BALDELPER,SCONFORM,SCONFORM_CN,RESPFORM_CN,DETAILCHECK,a.NODENUM
from b_mart.D_COMP_INVINF_MD5  a
left  join a_mart.A_BUS_ORG_ALL b on a.INVNAME = b.entname
where length(a.INVID) > 5  and length(a.entid) > 5 ;

truncate table a_mart.A_C03_INV_INF_FS;
--去掉自己投资自己的投资关系以及投资上市公司的关系
with aa as (select entid from a_mart.A_B04_COMP_TAG)
insert  overwrite table a_mart.A_C03_INV_INF_FS partition (NODENUM)
select a.DATA_DATE,a.ENTID,a.GDID,a.INVID,a.INVNAME,a.INVTYPE,c.pa_name,a.BLICTYPE,a.BLICTYPE_CN,a.BLICNO,a.ISCP,a.COUNTRY,
a.COUNTRY_CN,a.CURRENCY,a.SUBCONAM,a.SUBCONDATE,a.ACCCONAM,a.BALDELPER,a.SCONFORM,a.SCONFORM_CN,a.RESPFORM_CN,a.DETAILCHECK,a.NODENUM
from b_mart.B_C03_INV_INF_FS a
left join aa b on a.entid = b.entid
left join a_mart.a_a01_code_lib_cr c on a.INVTYPE = c.pa_code and c.itm_no = 'InvType' and c.itm_source = 'SAIC'
where  b.entid is null and a.entid != a.invid and a.iscp in ('C' ,'P');

--创建法人企业股东信息汇总表
truncate table e_mart.E_C01_BASE_INF_INCREA_FS;
--以工商企业为单位进行认缴金额和实缴金额汇总
insert into e_mart.E_C01_BASE_INF_INCREA_FS select entid ,sum(SUBCONAM) as TOTALREGCAP,sum(ACCCONAM) as ACCCONAM
from a_mart.A_C03_INV_INF_FS group by entid ;


--计算企业总资产，取认缴总额和注册资本中较大的值
truncate table e_mart.E_C01_BASE_INF_REG_SUM;

insert into e_mart.E_C01_BASE_INF_REG_SUM select a.entid,
case when a.regcap is  null or  lower(trim(a.regcap)) = 'null' then b.TOTALREGCAP
when cast(b.TOTALREGCAP as decimal(18,4)) is not null and
(cast(b.TOTALREGCAP as decimal(18,4)) > cast(a.regcap as decimal(18,4))) then b.TOTALREGCAP
else a.regcap end from a_mart.A_C01_BASE_INF_FS a
left join e_mart.E_C01_BASE_INF_INCREA_FS b on a.entid = b.entid;


--投资方认缴金额除以企业认缴总额得出投资比例
truncate table b_mart.B_D01_INV_REL_CR;

insert into b_mart.B_D01_INV_REL_CR
select a.ENTID ,a.INVID ,a.INVNAME ,a.ISCP ,a.CURRENCY ,a.SUBCONAM ,b.TOTALREGCAP,
case when a.subconam is null or b.TOTALREGCAP is null or lower(a.subconam) = 'null'
or lower(b.TOTALREGCAP) = 'null' or b.TOTALREGCAP = 0 or cast(b.TOTALREGCAP as double) is null then -1
else  cast(round(a.SUBCONAM/b.TOTALREGCAP,4) as decimal(18,4)) end
from a_mart.A_C03_INV_INF_FS  a
inner join e_mart.E_C01_BASE_INF_REG_SUM  b on a.ENTID = b.ENTID ;


--关联企业实体，获取投资关系
truncate table e_mart.E_D07_BUS_REL_CR;

insert into e_mart.E_D07_BUS_REL_CR partition (FS_Rel_Typ)
select regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") ,a.INVID ,a.INVNAME ,a.ENTID,b.ENTNAME,
a.FN_INV_PROP ,a.ISCP,trim(substr(CURRENT_TIMESTAMP,0,10)),'9999-12-31','','','','','Investment'
from b_mart.B_D01_INV_REL_CR  a inner join a_mart.a_C02_BUS_ENT_CR b on a.ENTID = b.ENTID;

--创建企业关系临时表
create table  e_mart.E_D07_BUS_REL_CR_bak like e_mart.E_D07_BUS_REL_CR;



--法人企业主要管理人员信息表关联企业实体表，获取任职关系
insert into e_mart.E_D07_BUS_REL_CR_bak partition (FS_Rel_Typ)
select regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") ,a.personid,a.pername,a.ENTID,b.ENTNAME,0,
case when c.entid is null then 'P' else 'C' end,
trim(substr(CURRENT_TIMESTAMP,0,10)),'9999-12-31',0,0.00,position_cn,null,'Header' from
a_mart.a_c05_person_inf_fs  a
inner join a_mart.a_C02_BUS_ENT_CR b on a.ENTID = b.ENTID
left join a_mart.A_BUS_ORG_ALL c on a.personid = c.entid
where length(a.entid)>10 and length(a.personid)>10 and length(a.position_cn) > 1;

--任职关系数据以fromid,toid,职位三个字段分类，以个人姓名排序去重后，数据插入企业关系表
insert into e_mart.E_D07_BUS_REL_CR partition(FS_Rel_Typ) select fs_id,fs_from_bus_id,fs_from_bus_nm,fs_to_bus_id,fs_to_bus_nm,fn_val_wt,fs_is_person,fd_crt_tm,fd_upd_tm,fn_backup_1,fn_backup_2,fs_backup_3,fs_backup_4,fs_rel_typ from (select fs_id,fs_from_bus_id,fs_from_bus_nm,fs_to_bus_id,fs_to_bus_nm,fn_val_wt,fs_is_person,fd_crt_tm,fd_upd_tm,fn_backup_1,fn_backup_2,fs_backup_3,fs_backup_4,fs_rel_typ,row_number() over (partition by fs_from_bus_id,fs_to_bus_id,fs_backup_3 order by fs_from_bus_nm desc) as sort  from e_mart.E_D07_BUS_REL_CR_bak) t where t.sort = 1;

--删除企业关系临时表
drop table e_mart.E_D07_BUS_REL_CR_bak;
--添加上市公司投资数据
insert into e_mart.e_d07_bus_rel_cr partition(FS_Rel_Typ)
select fs_id,fs_from_bus_id,fs_from_bus_nm,fs_to_bus_id,fs_to_bus_nm,fn_val_wt,fs_is_person,fd_crt_tm,fd_upd_tm,
fn_backup_1,fn_backup_2,fs_backup_3,fs_backup_4,fs_rel_typ from e_mart.E_D07_BUS_REL_CR_tag ;

--企业关系表中所有from节点插入节点临时表
truncate table e_mart.E_D07_BUS_NODE_MID_CR;

insert into e_mart.E_D07_BUS_NODE_MID_CR select concat('a',regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "")),a.FS_From_BUS_Id,a.FS_From_BUS_NM,b.REGCAP,a.FS_IS_PERSON,case when b.entid is not null then b.ENTSTATUS else '99' end as entstatus  from e_mart.E_D07_BUS_REL_CR a left join a_mart.A_C01_BASE_INF_FS b on a.FS_From_BUS_Id = b.entid ;
--企业关系表中所有to节点插入节点临时表
insert into e_mart.E_D07_BUS_NODE_MID_CR select concat('b',regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "")),a.FS_To_BUS_Id ,a.FS_To_BUS_Nm,b.REGCAP,'C',case when b.entid is not null then b.ENTSTATUS else '99' end as entstatus from e_mart.E_D07_BUS_REL_CR a left join a_mart.A_C01_BASE_INF_FS b on a.FS_To_BUS_Id = b.entid ;

--企业关系临时表数据以企业标识分类，企业名称排序后插入企业节点表，是否投资担保机构默认为'None'
truncate table  e_mart.E_D07_BUS_NODE_CR;
insert into e_mart.E_D07_BUS_NODE_CR (fs_id,entid,entname,regcap,iscp,inv_grt_typ,entstatus) select fs_id,entid,entname,regcap,iscp,'None',entstatus from (select fs_id,entid,entname,regcap,iscp,entstatus,row_number() over (partition by ENTID order by iscp,fs_id asc) as num  from e_mart.E_D07_BUS_NODE_MID_CR) t where t.num = 1;
truncate table  e_mart.E_D07_BUS_NODE_CR_bak;
--关联担保投资机构实体表，是否投资担保机构以实体表为准，关联上的数据插入关系节点临时表
insert into e_mart.E_D07_BUS_NODE_CR_bak (fs_id,entid,entname,regcap,iscp,inv_grt_typ,entstatus) select a.fs_id,a.entid,a.entname,a.regcap,a.iscp,b.type,a.entstatus from e_mart.E_D07_BUS_NODE_CR a  inner join b_mart.B_C10_INV_GRT_AGC_CR b on a.entname = b.name;
--此处应特别注意，若未设置该参数，则以下sql执行失败，没有数据。
set hive.strict.checks.cartesian.product=false;
set hive.mapred.mode=nonstrict;
--查询不在投资担保机构名单的数据，字段不变化，插入关系节点临时表
insert into e_mart.E_D07_BUS_NODE_CR_bak (fs_id,entid,entname,regcap,iscp,inv_grt_typ,entstatus)
select a.fs_id,a.entid,a.entname,a.regcap,a.iscp,a.inv_grt_typ,entstatus from e_mart.E_D07_BUS_NODE_CR a
where a.entname not in (select b.name from b_mart.B_C10_INV_GRT_AGC_CR b );
--关系节点临时表数据覆写到关系节点表
truncate table  e_mart.E_D07_BUS_NODE_CR;
insert into e_mart.E_D07_BUS_NODE_CR  select * from e_mart.E_D07_BUS_NODE_CR_bak ;

--矫正关系表中iscp字段

insert overwrite table  e_mart.e_d07_bus_rel_cr partition(FS_Rel_Typ)
select a.fs_id,a.fs_from_bus_id,a.fs_from_bus_nm,a.fs_to_bus_id,a.fs_to_bus_nm,a.fn_val_wt,b.iscp,a.fd_crt_tm,a.fd_upd_tm,
a.fn_backup_1,a.fn_backup_2,a.fs_backup_3,a.fs_backup_4,a.fs_rel_typ from e_mart.e_d07_bus_rel_cr a
inner join e_mart.E_D07_BUS_NODE_CR b on a.fs_from_bus_id = b.entid;

