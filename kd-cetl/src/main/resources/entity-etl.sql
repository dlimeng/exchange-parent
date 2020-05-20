set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set hive.execution.engine=tez;
set hive.tez.container.size=4096;


truncate table b_mart.B_A01_CODE_LIB_CR;

insert into b_mart.B_A01_CODE_LIB_CR  select DATA_DATE,ITM_SOURCE,ITM_NO,ITM_NAME,PA_CODE,PA_ID,PA_NAME,PA_LEVEL,PA_CODSUP  from ods_mart.D_DS_CODELIB;

truncate table a_mart.A_A01_CODE_LIB_CR;

insert into a_mart.A_A01_CODE_LIB_CR  select regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", ""),DATA_DATE,ITM_SOURCE,ITM_NO,ITM_NAME,PA_CODE,PA_ID,PA_NAME,PA_LEVEL,PA_CODSUP  from b_mart.B_A01_CODE_LIB_CR;

truncate table b_mart.B_C01_BASE_INF_FS;

with
cte as
(
    select pa_code,pa_name,itm_no from a_mart.a_a01_code_lib_cr
)
insert into b_mart.B_C01_BASE_INF_FS partition (NODENUM)
select case when lower(c1.data_date) = 'null' then null else c1.data_date end as  data_date, c1.entid,c1.entname,
 case when lower(c1.uniscid) = 'null' then null else c1.uniscid end as  uniscid,
 case when lower(c1.regno) = 'null' then null else c1.regno end as  regno,
 case when lower(c1.licid) = 'null' then null else c1.licid end as  licid,
 case when lower(c1.enttype) = 'null' then null else c1.enttype end as  enttype,
 ec.pa_name,
 case when lower(c1.industryphy) = 'null' then null else c1.industryphy end as  industryphy,
 case when lower(c1.industryco) = 'null' then null else c1.industryco end as  industryco,
 to_date(c1.opfrom) as opfrom,to_date(c1.opto)  as  opto,case when lower(c1.postalcode) = 'null' then null else c1.postalcode end as  postalcode,
 case when lower(c1.tel) = 'null' then null else c1.tel end as  tel,
 case when lower(c1.email) = 'null' then null else c1.email end as  email,
 to_date(c1.esdate)  as  esdate, to_date(c1.apprdate)  as  apprdate,
 case when lower(c1.regorg) = 'null' then null else c1.regorg end as  regorg,
 rc.pa_name,
 case when lower(c1.regcap) = 'null' then null else c1.regcap end as  regcap,
 null,case when lower(c1.entstatus) = 'null' then null else c1.entstatus end as  entstatus,
 case c1.entstatus  when '1' then '在营（开业）企业' when '2' then '吊销企业' when '3' then '注销企业' when '4' then '迁出' else '未知' end as entstatus_cn,
 case when lower(c1.opscope) = 'null' then null else c1.opscope end as  opscope,dc.pa_name,
 case when lower(c1.dom) = 'null' then null else c1.dom end as  dom,
 case when lower(c1.empnum) = 'null' then null else c1.empnum end as  empnum,
 case when lower(c1.sconform) = 'null' then null else c1.sconform end as  sconform,
 sc.pa_name,
 case when lower(c1.regcapcur) = 'null' then null else c1.regcapcur end as  regcapcur,
 rec.pa_name,
 case when lower(c1.country) = 'null' then null else c1.country end as  country,cc.pa_name,
 case when lower(c1.lerepname) = 'null' then null else c1.lerepname end as  lerepname,
 case when lower(c1.personid) = 'null' then null else c1.personid end as  personid,to_date(c1.candate)  as  candate,
 case when lower(c1.canreason) = 'null' then null else c1.canreason end as  canreason,to_date(c1.revdate)  as  revdate,
 case when lower(c1.revreason) = 'null' then null else c1.revreason end as  revreason, to_date(c1.altdate)  as  altdate,
 case when lower(c1.inv_nums) = 'null' then null else c1.inv_nums end as  inv_nums,
 case when lower(c1.person_nums) = 'null' then null else c1.person_nums end as  person_nums,null,c1.nodenum from
 ods_mart.d_comp_baseinf  c1
 left join cte ec on ec.pa_code = c1.enttype and ec.itm_no = 'EntType'
 left join cte rc on rc.pa_code = c1.regorg and rc.itm_no = 'Regorg'
 left join cte dc on dc.pa_code = c1.opscope and dc.itm_no = 'RegionCode'
 left join cte sc  on sc.pa_code = c1.sconform and sc.itm_no = 'SconForm'
 left join cte rec on rec.pa_code = c1.regcapcur and rec.itm_no = 'CurrencyCode'
 left join cte cc on cc.pa_code = c1.country and cc.itm_no = 'CountryCode';

truncate table a_mart.A_C01_BASE_INF_FS;

insert into a_mart.A_C01_BASE_INF_FS partition (NODENUM) select c.DATA_DATE, ENTID,
regexp_replace(NVL(trim(entname),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as ENTNAME ,UNISCID ,
regexp_replace(NVL(trim(regno),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as regno ,
LICID , ENTTYPE , ENTTYPE_CN,INDUSTRYPHY , INDUSTRYCO , c.OPFROM  as  OPFROM,c.OPTO  as  OPTO,
regexp_replace(NVL(trim(postalcode),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as POSTALCODE ,
regexp_replace(NVL(trim(tel),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as TEL ,
regexp_replace(NVL(trim(email),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as EMAIL ,c.ESDATE as  ESDATE,
c.APPRDATE  as  APPRDATE,REGORG,REGORG_CN  , REGCAP,'',ENTSTATUS , ENTSTATUS_CN ,
regexp_replace(NVL(trim(opscope),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as OPSCOPE,DOMDISTRICT,
regexp_replace(NVL(trim(dom),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as DOM , EMPNUM  , SCONFORM , SCONFORM_CN ,
regexp_replace(NVL(trim(regcapcur),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as REGCAPCUR ,
regexp_replace(NVL(trim(REGCAPCUR_CN),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as REGCAPCUR_CN,COUNTRY,COUNTRY_CN,
regexp_replace(NVL(trim(lerepname),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as LEREPNAME ,PERSONID ,c.CANDATE as  CANDATE,
regexp_replace(NVL(trim(canreason),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as CANREASON ,c.REVDATE  as  REVDATE,
regexp_replace(NVL(trim(REVREASON),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as REVREASON ,c.DATA_DATE  as  ALTDATE ,
INV_NUMS , PERSON_NUMS,cd.pa_name as industry_desc, NODENUM   from b_mart.B_C01_BASE_INF_FS c left join a_mart.A_A01_CODE_LIB_CR cd on cd.itm_no = 'IndustryCode' and cd.pa_id = c.industryco and cd.pa_id !='null'  and cd.pa_id is not null;

--工商照面中按照企业标识分类并按照企业名称排序后取企业标识、企业名称、省份三个字段，取sort==1的数据
truncate table a_mart.a_C02_BUS_ENT_CR;
insert into a_mart.a_C02_BUS_ENT_CR select t.entid,t.entname,t.nodenum from
(select a.entid,a.entname,a.nodenum,row_number() over (partition by a.entname order by a.ENTID desc) as sort
from a_mart.a_c01_base_inf_fs a ) t where t.sort = 1;

truncate table b_mart.D_COMP_INVINF_MD5;

insert into b_mart.D_COMP_INVINF_MD5 select data_date,nodenum,entid,gdid,case when lower(invid) = 'null' then md5(concat(entid,trim(invname))) else invid end ,regexp_replace(NVL(trim(INVNAME),''),'[\\s＊〓\\\"]|[\\u0000-\\u001f]','') as INVNAME,invtype,invtype_cn,blictype,blictype_cn,blicno,country,country_cn,currency,subconam,subcondate,accconam,baldelper,iscp,detailcheck,sconform,sconform_cn,respform_cn,jobid,domid from ods_mart.D_COMP_INVINF where length(trim(invname)) > 1 and lower(trim(invname)) != 'null';

truncate table b_mart.D_COMP_PERSONINF_MD5;

insert into b_mart.D_COMP_PERSONINF_MD5 select data_date,nodenum,entid,case when lower(personid) = 'null' then md5(concat(entid,trim(pername))) else personid end as personid,pername,certype,cerno,tel,country,lerepsign,position,position_cn,jobid,domid  from ODS_MART.D_COMP_PERSONINF where length(trim(pername)) > 1 and lower(trim(pername)) != 'null';

truncate table b_mart.B_C05_PERSON_INF_FS;

with aa as(select * from A_MART.A_A01_CODE_LIB_CR where ITM_SOURCE = 'SAIC' and ITM_NO = 'Position'  )
insert into b_mart.B_C05_PERSON_INF_FS partition (NODENUM)
select  A.DATA_DATE,A.ENTID,A.PERSONID,regexp_replace(NVL(trim(a.pername),''),'[\\s|\\\"\b\t]|[\\u0000-\\u001f]','') as pername,
A.CERTYPE,A.CERNO,A.TEL,A.COUNTRY,A.LEREPSIGN,A.POSITION,
case when A.LEREPSIGN = '1' then '法定代表人' else B.PA_NAME end as POSITION_CN,A.NODENUM
from b_mart.D_COMP_PERSONINF_MD5 A
left JOIN aa B  ON A.POSITION = B.PA_CODE and length(A.entid)>1 and length(A.pername)>1 and length(A.POSITION)>1;

truncate table a_mart.A_C05_PERSON_INF_FS;

insert into a_mart.A_C05_PERSON_INF_FS partition (NODENUM) select regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", ""),t.DATA_DATE ,t.ENTID,t.PERSONID,t.PERNAME,t.CERTYPE,t.CERNO,t.TEL,t.COUNTRY,t.LEREPSIGN,t.POSITION,t.POSITION_CN,t.NODENUM from (select  A.DATA_DATE ,A.ENTID,A.PERSONID,A.PERNAME,A.CERTYPE,A.CERNO,A.TEL,A.COUNTRY,A.LEREPSIGN,A.POSITION,A.POSITION_CN,A.NODENUM,row_number() over (partition by a.ENTID,a.PERNAME,a.POSITION order by a.DATA_DATE desc) as sort from b_mart.B_C05_PERSON_INF_FS a ) t where t.sort = 1;

truncate table b_mart.B_C11_ORG_BASIC_INF_FS;

insert into b_mart.B_C11_ORG_BASIC_INF_FS select *  from ods_mart.D_ORG_BASICINF a where a.ENTID not in (select b.entid from a_mart.A_C02_BUS_ENT_CR b );

truncate table a_mart.a_C11_ORG_BASIC_INF_FS;

insert into  a_mart.a_C11_ORG_BASIC_INF_FS select data_date,entid,uniscid,orgcode,orgcerno, regioncode ,regexp_replace(NVL(trim(orgname),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as   orgname , orgtype ,ischaritable , issauth , issauthname, regdate, chargorg, chargname ,orgstatus,orgfrom,orgto ,bizaddr ,bizscope ,tel,fax,email,website,regcap,regcapunit ,regcapcurr ,lerepname,jobid from b_mart.B_C11_ORG_BASIC_INF_FS;

truncate table a_mart.A_C06_PERSON_ENT_CR_bak;
--法人企业主要管理人员信息表中以个人标识分类并按照个人姓名排序后，取企业个人标识、个人姓名、省份三个字段，取sort==1的数据
insert into a_mart.A_C06_PERSON_ENT_CR_bak
select t.PERSONID,t.PERNAME,t.nodenum from (select a.PERSONID,a.PERNAME,a.nodenum,row_number()
over (partition by a.pername,a.entid order by a.personid desc) as sort
from a_mart.A_C05_PERSON_INF_FS a where a.personid is not null and a.PERNAME is not null and
a.personid != '' and a.PERNAME != '') t where t.sort = 1;


--个人企业实体中间表中数据去重后，取sort==1的数据
truncate table a_mart.A_C06_PERSON_ENT_CR;
insert into a_mart.A_C06_PERSON_ENT_CR select t.PERSONID,t.PERNAME,t.nodenum from (select a.PERSONID,a.PERNAME,a.nodenum,row_number() over (partition by a.PERSONID order by a.PERNAME desc) as sort  from a_mart.A_C06_PERSON_ENT_CR_bak a ) t left join a_mart.a_C02_BUS_ENT_CR b on  t.personid = b.entid where t.sort = 1 and b.entid is null;

--组织机构基本信息表中取企业标识、企业名称
truncate table a_mart.A_C11_ORG_BASIC_ENT_FS;
insert into a_mart.A_C11_ORG_BASIC_ENT_FS select entid ,orgname from a_mart.a_C11_ORG_BASIC_INF_FS;

--所有未匹配实体进入待正则匹配实体表
truncate table a_mart.A_ENT_ALL_REGEX;
with aa as (select invid,invname from
(select invid,invname ,row_number() over (partition by invid order by invname desc) as sort
from b_mart.D_COMP_INVINF_MD5) t where length(t.invid) > 5 and length(t.invname) > 1 and t.sort = 1)
insert into a_mart.A_ENT_ALL_REGEX
select invid,invname,'' from aa a
left join a_mart.a_C02_BUS_ENT_CR b on a.invid = b.entid
left join a_mart.A_C06_PERSON_ENT_CR c on a.invid = c.PERSONID
left join a_mart.A_C11_ORG_BASIC_ENT_FS d on a.invid = d.entid
where b.entid is null and c.personid is null and d.entid is null;

--匹配非注册企业
truncate table a_mart.A_C02_FOREIGN_BUS_ENT_CR;
insert into a_mart.A_C02_FOREIGN_BUS_ENT_CR select  entid, entname, nodenum from a_mart.A_ENT_ALL_REGEX where lower(trim(entname)) regexp 'limited|co.|ltd|comp|inc.|llc|会社|洋行|宾馆|煤场|酒家|合伙企业|画院|商场|旅行社|盐场|超市|运输场|疗养院|新华书店|事务所|大学|供应站|良种场|原种场|企业|集团|搅拌站|商行|康复院|杂志社|总台|渔场|菜场|技校|影音部|图片社|繁殖场|林场|出版社|客运站|总场|繁育场|种子站|商社|餐厅|华创赢达|宽鼎资产|今晚报社|发达汇|中亿伟业|伟瑞发展|深创投|深圳创投';
insert into a_mart.A_C02_FOREIGN_BUS_ENT_CR select  entid, entname, nodenum from a_mart.A_ENT_ALL_REGEX where lower(trim(entname)) regexp '^*.厂$|^*.店$|^*.公司$' and length(entname) > 3 ;





--匹配正则类组织机构
truncate table a_mart.A_C11_ORG_BASIC_ENT_FS_regex;
insert into a_mart.A_C11_ORG_BASIC_ENT_FS_regex select  entid, entname from a_mart.A_ENT_ALL_REGEX where lower(trim(entname)) regexp '财政部|政府|林业局|村委会|建设局|供销社|贸易局|贸易行|委员会|办事处|中学|管理处|乡经委|电力局|管理站|管理局|水电站|林业站|经营部|联合会|研究所|研究院|研究室|办公室|服务站|服务部|服务中心|残联|总厂|总站|监督局|工会|合作社管理所|合作联社|合作社|信用社|联合社|推广站|工业局|工程局|工程处|工业社|粮食局|联谊会|教育馆|教育局|商业局|档案馆|银行|协会|物资站|矿务局|管委|房管所|文化局|土地局|中心|副食店|乡企办|植保站|财政局|渔业局|通信总站|交通局|物资局|水果局|旅游局|运营中心|企办室|学校|茶果场|科研所|旅社|建委|邮政局|检测站|设计院|食品局|科技开发部|校友会|支行|社区|农机局|电信局|基金|经济局|保护局|环保局|乡镇局|商务局|交通部|勘查局|农业局|民政局|国资委|国务院|农林局|分局|工厂|电业局|水利局|卫生局|企业局|水务局|供电局|电视局|妇保所|情报局|海事局|事务局|开发局|法制局|铁路局|地税局|税务局|公安局|城建局|勘察院|国土局|水产局|国资局|小学|经贸局|勘探局|气象局|科技局|国土资源局|出版局|商贸局|工信局|物质局|招商局|果业局|改革局|医药局|审计局|就业局|管理所|科学技术局|事业局|水电局|地质局|发展局|联络局|机电局|医药管理局|运输局|医院|邮电局|规划局|经济管理局|企业管理局|服务局|乡企局|工商局|财税局|老干局|地矿局|农电局|水表局|体育局|省计局|房产局|总局|药监局|人事局|劳动局|公路局|烟草专卖局|煤炭局|生育局|畜牧局|华管局|二轻局|黄金局|农垦局|矿产局|建工局|信息化局|监察局|物价局|移民局|农水局|老干部局|招标局|文物局|海洋局|住建局|检疫局|一商局|二商局|勘测局|博览局|基金会|医局|统计局|农牧局|档案局|石油局|盐务局|文教局|计量局|司法局|文体局|轻功局|测绘局|影视局|国税局|水一局|保障局|教科文局|管局|森工局|安监局|安检局|技术局|地震局|合作局|农场局|广电局|资产局|征收局|手管局|纺工局|林米局|劳改局|林管局|园林局|发改委|警卫局|计划局|线路局|冶金局|教体局|地勘局|三管局|水库局|专卖局|航天局|企业委|县建委|市建委|村委|省建委|海关|促进会|居委会|居委|质监站|电视台|行政公署|通信段|检疫站|报社|开发部|开发办|后勤部|联社|围垦局|武装部|招待所|粮管站|戒毒所|技协|教堂|妇联|学院|研究总院|资源局|商联|供应站|司法厅|老龄委|就业所|财政所|文化馆|计经委|交易所|农科所|殡仪馆|规划院|计委|经贸委|运管处|文化站|兽医站|批发站|党校|企业办|财务局|经理部|领导组|广告部|接待处|乐经委|农电站|农科院|经营站|总社|编辑部|通信局|影视台|城粮所|企管站|粮库|农机站|农经委|设计室|经销处|经销部|博物馆|公路段|指挥部|经济社|水保局|敬老院|俱乐部|服务所|商会|农办|发射台|造价站|供热办|组织部|经管办|分院|水利厅|分会|河务处|工作站|检验所|贸易部|图书馆|煤矿|街道办|体委|经联委|管理委员会|经联社|房监所|粮站|税务处|公路管理总段|水利站|休养所|蔬菜局|清真寺|辐射厅|经营处|机械局|药检所|企业站|养护段|农技站|水土保持局|河务局|分社|发行站|环卫处|公路管理段|检测所|供应据|门诊部|派出所|装备部|农委|公路总段|社公办|生产办|电视站|监理所|设计所|科协|少年宫|军分区|保障所|航运局|稽查局|总务处|解放军|学会|管理部|供电所|侨务局|艺术馆|咨询部|研究会|储备库|农经站|705所|联络处|勘查院|总会|服务处|水保办|福利院|幼儿园|金融局|交通厅|监督站|筹建组|文化宫|征收所|工办|粮管所|退管会|企业司|质监局|管理段|安全局|经信委|电气化局|经管站|车务段|慈善会|劳司|纪念馆|安全厅|基地|商社|公交局|协作办|展销厅|规划处|养老院|贸易厅|工委|测试所|办公厅|民政厅|工业和信息化厅|镭射厅|外贸厅|工业厅|林业厅|商务厅|研究厅|文化厅|公安厅|劳动厅|粮食厅|劳教处|教育厅|卫生厅|信息产业厅|商业厅|情报厅|农牧厅|农业厅|建设厅|合作厅|人事厅|团委|社会保障厅|审计厅|石化厅|煤炭厅|农垦厅|机电厅|电视厅|国土资源厅|经贸厅|化工厅|轻工厅';
insert into a_mart.A_C11_ORG_BASIC_ENT_FS_regex select  entid, entname from a_mart.A_ENT_ALL_REGEX where lower(trim(entname)) regexp '^*.队$|^*.团$|^*.村$' and length(entname) > 3;

--匹配正则类自然人
truncate table a_mart.A_C06_PERSON_ENT_CR_regex;
insert into a_mart.A_C06_PERSON_ENT_CR_regex select  a.entid, a.entname,a.nodenum from a_mart.A_ENT_ALL_REGEX a left join a_mart.A_C02_FOREIGN_BUS_ENT_CR b on a.entid = b.entid left join a_mart.A_C11_ORG_BASIC_ENT_FS_regex c on a.entid = c.entid where b.entid is null and c.entid is null;

truncate table a_mart.A_BUS_ORG_ALL;

insert into a_mart.A_BUS_ORG_ALL select entid, regexp_replace(NVL(trim(entname),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as entname,nodenum from a_mart.a_C02_BUS_ENT_CR;
insert into a_mart.A_BUS_ORG_ALL select entid, regexp_replace(NVL(trim(entname),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as entname,nodenum from a_mart.A_C02_FOREIGN_BUS_ENT_CR;
insert into a_mart.A_BUS_ORG_ALL select entid, regexp_replace(NVL(trim(orgname),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as entname,'' from a_mart.A_C11_ORG_BASIC_ENT_FS;
insert into a_mart.A_BUS_ORG_ALL select entid, regexp_replace(NVL(trim(orgname),''),'[\\s|\\\|\"|\b|\t|！|!]|[\\u0000-\\u001f]','') as entname,'' from a_mart.A_C11_ORG_BASIC_ENT_FS_regex;

--全部实体数据去重
insert overwrite table a_mart.A_BUS_ORG_ALL select t.entid,t.entname,t.nodenum from (select entid,entname,nodenum,row_number() over (partition by a.entname order by a.nodenum desc ) as sort from a_mart.A_BUS_ORG_ALL a) t where t.sort = 1;
