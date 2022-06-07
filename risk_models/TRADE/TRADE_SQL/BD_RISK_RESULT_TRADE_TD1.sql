
CREATE TABLE BD_RISK_RESULT_TRADE_TD1 (
    ID NUMBER(20) NOT NULL  ,
    ORG_CODE VARCHAR2(64) NOT NULL ,
    ORG_NAME VARCHAR2(64) NULL ,
    CASE_TYPE VARCHAR2(64) NULL ,
    CASE_TITLE VARCHAR2(512) NULL ,
    CASE_DATE DATE,
    CUS_CODE  VARCHAR2(64) NULL,
    LEGAL_REP  VARCHAR2(64) NULL,
    ENTRY_NUM  VARCHAR2(64) NULL,
    SCORE NUMBER(20,5) NULL,
    CHECK_TIME DATE NOT NULL,
    ISCURRENT NUMBER(10) NOT NULL,
    LASTUPDATE DATE NOT NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD1.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD1.ORG_CODE IS '经营企业信用代码';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD1.ORG_NAME  IS '经营企业名称';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD1.CASE_TYPE  IS '案件类型';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD1.CASE_TITLE  IS '案件标题';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD1.CASE_DATE  IS '案件日期';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD1.CUS_CODE  IS '海关企业编号';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD1.LEGAL_REP  IS '法人代表';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD1.SCORE IS '模型扣分';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD1.CHECK_TIME IS '模型运行时间';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD1.ISCURRENT IS '是否使用：1(使用)；0(不使用)';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD1.LASTUPDATE IS '数据最后更新时间';

ALTER TABLE BD_RISK_RESULT_TRADE_TD1 ADD CHECK (ID IS NOT NULL);

ALTER TABLE BD_RISK_RESULT_TRADE_TD1 ADD PRIMARY KEY (ID);