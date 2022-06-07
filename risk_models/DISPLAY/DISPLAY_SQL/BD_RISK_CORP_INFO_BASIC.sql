create table BD_RISK_CORP_INFO_BASIC
(
	ID NUMBER(20) not null
		constraint PK_BD_RISK_CORP_INFO_BASIC
			primary key,
	ID_FK NUMBER(20),
	ORG_CODE VARCHAR2(100),
	ORG_NAME VARCHAR2(500),
	I_E_METHOD VARCHAR2(50),
	CUS_QUA VARCHAR2(200),
	DANGER_GOODS VARCHAR2(50),
	COLD_FOOD VARCHAR2(50),
	EST_YEARS VARCHAR2(50),
	REG_CAPITAL VARCHAR2(50),
	I_E_TYPE VARCHAR2(50),
	CORP_TYPE VARCHAR2(200),
	INDUSTRY VARCHAR2(50),
	TAX_RATE VARCHAR2(50),
	CUS_RATE VARCHAR2(50),
	EXC_RATE VARCHAR2(50),
	ADDRESS VARCHAR2(2000),
	LON VARCHAR2(100),
	LAT VARCHAR2(100),
	REG_NO VARCHAR2(500),
	CORP_COUNTRY VARCHAR2(500),
	SUB_OBJ VARCHAR2(500),
	HYMC VARCHAR2(500),
	BUSINESS_SCOPE VARCHAR2(3000),
	BUSINESS_ADDRESS NVARCHAR2(500),
	AREA_CODE VARCHAR2(500),
	ZIP VARCHAR2(500),
	TELEPHONE VARCHAR2(500),
	ESTABLISH_DATE DATE,
	CURRENCY VARCHAR2(500),
	PERSON_NAME VARCHAR2(500),
	PERSON_CERT_TYPE VARCHAR2(500),
	PERSON_CERT_CODE VARCHAR2(500),
	CORP_STATUS VARCHAR2(500),
	PERSON_EMAIL VARCHAR2(500),
	PERSON_LANDLINE_TEL VARCHAR2(500),
	CHANGE_DATE DATE,
	CHANGE_ITEM VARCHAR2(500),
	CONTACT_MOBILE VARCHAR2(500),
	CONTACT_EMAIL VARCHAR2(500),
	CONTACT_CER_TYPE VARCHAR2(500),
	CONTACT_CER_NO VARCHAR2(500),
	DWCBRS VARCHAR2(500),
	JNJE VARCHAR2(500),
	SFQJ VARCHAR2(500),
	CBNY VARCHAR2(500),
	ZZZGPJRS VARCHAR2(500),
	PDDJ VARCHAR2(500),
	MTYPE VARCHAR2(500),
	ISCURRENT NUMBER(10),
	LASTUPDATE DATE,
	CHECK_TIME DATE
)
/

comment on table BD_RISK_CORP_INFO_BASIC is '风控企业基本信息表'
/

comment on column BD_RISK_CORP_INFO_BASIC.ID is '自增长ID'
/

comment on column BD_RISK_CORP_INFO_BASIC.ID_FK is '外键ID'
/

comment on column BD_RISK_CORP_INFO_BASIC.ORG_CODE is '企业18位信用代码'
/

comment on column BD_RISK_CORP_INFO_BASIC.ORG_NAME is '企业名称'
/

comment on column BD_RISK_CORP_INFO_BASIC.I_E_METHOD is '进出口方式'
/

comment on column BD_RISK_CORP_INFO_BASIC.CUS_QUA is '报关资格'
/

comment on column BD_RISK_CORP_INFO_BASIC.DANGER_GOODS is '是否经营危险品'
/

comment on column BD_RISK_CORP_INFO_BASIC.COLD_FOOD is '是否从事冷链食品'
/

comment on column BD_RISK_CORP_INFO_BASIC.EST_YEARS is '成立年限'
/

comment on column BD_RISK_CORP_INFO_BASIC.REG_CAPITAL is '注册资本'
/

comment on column BD_RISK_CORP_INFO_BASIC.I_E_TYPE is '进出口类型'
/

comment on column BD_RISK_CORP_INFO_BASIC.CORP_TYPE is '企业类型'
/

comment on column BD_RISK_CORP_INFO_BASIC.INDUSTRY is '所属行业'
/

comment on column BD_RISK_CORP_INFO_BASIC.TAX_RATE is '纳税信用等级'
/

comment on column BD_RISK_CORP_INFO_BASIC.CUS_RATE is '海关信用等级'
/

comment on column BD_RISK_CORP_INFO_BASIC.EXC_RATE is '外汇管理信用等级'
/

comment on column BD_RISK_CORP_INFO_BASIC.ISCURRENT is '是否使用'
/

comment on column BD_RISK_CORP_INFO_BASIC.LASTUPDATE is '最后修改时间'
/

comment on column BD_RISK_CORP_INFO_BASIC.CHECK_TIME is '模型运行时间'
/

comment on column BD_RISK_CORP_INFO_BASIC.ADDRESS is '企业注册地址；住所'
/

comment on column BD_RISK_CORP_INFO_BASIC.LON is '经度'
/

comment on column BD_RISK_CORP_INFO_BASIC.LAT is '纬度'
/

comment on column BD_RISK_CORP_INFO_BASIC.REG_NO is '工商注册号'
/

comment on column BD_RISK_CORP_INFO_BASIC.CORP_COUNTRY is '公司国别'
/

comment on column BD_RISK_CORP_INFO_BASIC.SUB_OBJ is '企业大类'
/

comment on column BD_RISK_CORP_INFO_BASIC.HYMC is '所属行业'
/

comment on column BD_RISK_CORP_INFO_BASIC.BUSINESS_SCOPE is '经营范围'
/

comment on column BD_RISK_CORP_INFO_BASIC.BUSINESS_ADDRESS is '实际经营地址'
/

comment on column BD_RISK_CORP_INFO_BASIC.AREA_CODE is '区域划分'
/

comment on column BD_RISK_CORP_INFO_BASIC.ZIP is '邮编'
/

comment on column BD_RISK_CORP_INFO_BASIC.TELEPHONE is '联系电话'
/

comment on column BD_RISK_CORP_INFO_BASIC.ESTABLISH_DATE is '成立日期'
/

comment on column BD_RISK_CORP_INFO_BASIC.CURRENCY is '币种'
/

comment on column BD_RISK_CORP_INFO_BASIC.PERSON_NAME is '法定代表人'
/

comment on column BD_RISK_CORP_INFO_BASIC.PERSON_CERT_TYPE is '法定代表人证件类型'
/

comment on column BD_RISK_CORP_INFO_BASIC.PERSON_CERT_CODE is '法定代表人证件号'
/

comment on column BD_RISK_CORP_INFO_BASIC.CORP_STATUS is '法人状态'
/

comment on column BD_RISK_CORP_INFO_BASIC.PERSON_EMAIL is '法人邮箱'
/

comment on column BD_RISK_CORP_INFO_BASIC.PERSON_LANDLINE_TEL is '固定电话'
/

comment on column BD_RISK_CORP_INFO_BASIC.CHANGE_DATE is '法人变更登记时间'
/

comment on column BD_RISK_CORP_INFO_BASIC.CHANGE_ITEM is '法人变更登记事项'
/

comment on column BD_RISK_CORP_INFO_BASIC.CONTACT_MOBILE is '风控企业基本信息表'
/

comment on column BD_RISK_CORP_INFO_BASIC.CONTACT_EMAIL is '工商联络员手机号码'
/

comment on column BD_RISK_CORP_INFO_BASIC.CONTACT_CER_TYPE is '工商联络员证件类型'
/

comment on column BD_RISK_CORP_INFO_BASIC.CONTACT_CER_NO is '工商联络员证件号码'
/

comment on column BD_RISK_CORP_INFO_BASIC.DWCBRS is '参保人数'
/

comment on column BD_RISK_CORP_INFO_BASIC.JNJE is '缴纳金额'
/

comment on column BD_RISK_CORP_INFO_BASIC.SFQJ is '是否欠缴'
/

comment on column BD_RISK_CORP_INFO_BASIC.CBNY is '年月'
/

comment on column BD_RISK_CORP_INFO_BASIC.ZZZGPJRS is '在职职工年平均人数'
/

comment on column BD_RISK_CORP_INFO_BASIC.PDDJ is '评定等级'
/

comment on column BD_RISK_CORP_INFO_BASIC.MTYPE is '海关信用评级'
/



