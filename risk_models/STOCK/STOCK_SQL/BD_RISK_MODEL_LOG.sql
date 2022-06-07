create table BD_RISK_MODEL_LOG
(
   ID NUMBER(20) not null,
   TASK_ID VARCHAR2(64) not null,
   CHILD_TASK_ID VARCHAR2(64) not null,
   MODEL_CODE VARCHAR2(64) not null,
   CHILD_MODEL_CODE VARCHAR2(64) not null,
   ORG_CODE VARCHAR2(64),
   START_TIME DATE,
   END_TIME DATE,
   EXEC_PARAM VARCHAR2(2000),
   EXEC_STATUS NUMBER(1) default null,
   LOG_MSG VARCHAR2(3000),
   BASE_TIME DATE,
   CREATE_TIME DATE default SYSDATE not null,
   UPDATE_TIME DATE default SYSDATE not null
)
/

comment on table BD_RISK_MODEL_LOG is '模型执行日志表'
/

comment on column BD_RISK_MODEL_LOG.ID is '主键id'
/

comment on column BD_RISK_MODEL_LOG.TASK_ID is '主任务id'
/

comment on column BD_RISK_MODEL_LOG.CHILD_TASK_ID is '子任务id'
/

comment on column BD_RISK_MODEL_LOG.MODEL_CODE is '模型code'
/

comment on column BD_RISK_MODEL_LOG.CHILD_MODEL_CODE is '子模型任务code'
/

comment on column BD_RISK_MODEL_LOG.ORG_CODE is '企业代号'
/

comment on column BD_RISK_MODEL_LOG.START_TIME is '任务开始时间'
/

comment on column BD_RISK_MODEL_LOG.END_TIME is '任务结束时间'
/

comment on column BD_RISK_MODEL_LOG.EXEC_PARAM is '执行参数'
/

comment on column BD_RISK_MODEL_LOG.EXEC_STATUS is '执行结果 0成功 1失败'
/
comment on column BD_RISK_MODEL_LOG.BASE_TIME is '执行时间'
/
comment on column BD_RISK_MODEL_LOG.LOG_MSG is 'log消息体内容'
/

comment on column BD_RISK_MODEL_LOG.CREATE_TIME is '创建时间'
/

comment on column BD_RISK_MODEL_LOG.UPDATE_TIME is '更新时间'
/

create unique index BD_RISK_MODEL_LOG_ID_UINDEX
   on BD_RISK_MODEL_LOG (ID)
/

create index BD_RISK_MODEL_LOG_O_INDEX
   on BD_RISK_MODEL_LOG (ORG_CODE)
/

create index BD_RISK_MODEL_LOG_T_INDEX
   on BD_RISK_MODEL_LOG (CHILD_TASK_ID, TASK_ID)
/

alter table BD_RISK_MODEL_LOG
   add constraint BD_RISK_MODEL_LOG_PK
      primary key (ID)
/
