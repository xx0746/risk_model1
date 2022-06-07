
create sequence DW_CUS_RC.SEQ_BD_RISK_LOGISTIC_GAODE_GPS
minvalue 1
maxvalue 9999999999999999999999999999
start with 1
increment by 1
cache 20;



create table DW_CUS_RC.BD_RISK_LOGISTIC_GAODE_GPS
(
  ID               NUMBER(15) default "DW_CUS_RC"."SEQ_BD_RISK_LOGISTIC_GAODE_GPS"."NEXTVAL" not null,
  BEGIN_POINT_NAME VARCHAR2(255),
  END_POINT_NAME   VARCHAR2(255),
  PATH_TYPE        VARCHAR2(1),
  STRATEGY         VARCHAR2(100),
  ROUTE            CLOB,
  DURATION         NUMBER(10),
  BUSINESS_TIME    VARCHAR2(20),
  LASTUPDATE       DATE default sysdate
);

alter table DW_CUS_RC.BD_RISK_LOGISTIC_GAODE_GPS
  add constraint PK_BD_RISK_LOGISTIC_GAODE_GPS primary key (ID);
  
  
-- Add comments to the table 
comment on table DW_CUS_RC.BD_RISK_LOGISTIC_GAODE_GPS
  is '高德点到点预估路径';
-- Add comments to the columns 
comment on column DW_CUS_RC.BD_RISK_LOGISTIC_GAODE_GPS.ID
  is '流水号';
comment on column DW_CUS_RC.BD_RISK_LOGISTIC_GAODE_GPS.BEGIN_POINT_NAME
  is '起点名称';
comment on column DW_CUS_RC.BD_RISK_LOGISTIC_GAODE_GPS.END_POINT_NAME
  is '终点名称';
comment on column DW_CUS_RC.BD_RISK_LOGISTIC_GAODE_GPS.PATH_TYPE
  is '路线类型（是否高峰期）1 - 高峰期数据 ,0 - 非高峰期数据';
comment on column DW_CUS_RC.BD_RISK_LOGISTIC_GAODE_GPS.STRATEGY
  is '导航策略';
comment on column DW_CUS_RC.BD_RISK_LOGISTIC_GAODE_GPS.ROUTE
  is '导航路径点信息';
comment on column DW_CUS_RC.BD_RISK_LOGISTIC_GAODE_GPS.DURATION
  is '预估行程时间（秒）';
comment on column DW_CUS_RC.BD_RISK_LOGISTIC_GAODE_GPS.BUSINESS_TIME
  is '导航信息调用时间';
comment on column DW_CUS_RC.BD_RISK_LOGISTIC_GAODE_GPS.LASTUPDATE
  is '数据更新时间';