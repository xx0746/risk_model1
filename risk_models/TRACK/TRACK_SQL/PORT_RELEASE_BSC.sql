create table DW_CUS_RC.PORT_RELEASE_BSC
(
  ID                 NUMBER(10) default "DW_CUS_RC"."SEQ_PORT_RELEASE_BSC"."NEXTVAL" not null,
  VEHICLE_NO         VARCHAR2(64 CHAR),
  IN_EXP_TYPE        VARCHAR2(1 CHAR),
  BIZOP_ETPS_NO      VARCHAR2(10 CHAR),
  BIZOP_ETPS_NM      VARCHAR2(768 CHAR),
  BIZOP_ETPS_SCCD    VARCHAR2(18 CHAR),
  IE_TYPECD          VARCHAR2(2 CHAR),
  PASS_TIME          DATE,
  PORT_IOCHKPT_STUCD VARCHAR2(1),
  AREA               VARCHAR2(1),
  ISCURRENT          NUMBER(1),
  PREENT_NO          VARCHAR2(2000)
);

-- Add comments to the columns 
comment on column DW_CUS_RC.PORT_RELEASE_BSC.VEHICLE_NO
  is '车牌号';
comment on column DW_CUS_RC.PORT_RELEASE_BSC.IN_EXP_TYPE
  is '出入库类型';
comment on column DW_CUS_RC.PORT_RELEASE_BSC.BIZOP_ETPS_NO
  is '海关10位编码';
comment on column DW_CUS_RC.PORT_RELEASE_BSC.BIZOP_ETPS_NM
  is '企业名称';
comment on column DW_CUS_RC.PORT_RELEASE_BSC.BIZOP_ETPS_SCCD
  is '企业信用代码';
comment on column DW_CUS_RC.PORT_RELEASE_BSC.IE_TYPECD
  is '进出标识';
comment on column DW_CUS_RC.PORT_RELEASE_BSC.PASS_TIME
  is '过卡时间';
comment on column DW_CUS_RC.PORT_RELEASE_BSC.PREENT_NO
  is '随车单证号';
  
-- Create/Recreate primary, unique and foreign key constraints 
alter table DW_CUS_RC.PORT_RELEASE_BSC
  add constraint PK_PORT_RELEASE_BSC primary key (ID);
-- Create/Recreate indexes 
create index IDX_PORT_RELEASE_BSC_PST on DW_CUS_RC.PORT_RELEASE_BSC (PASS_TIME);
