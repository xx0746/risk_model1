
CREATE TABLE BD_RISK_RESULT_TRADE_TD2 (
    ID NUMBER(20) NOT NULL  ,
    ORG_CODE VARCHAR2(64 CHAR) NOT NULL ,
    ORG_NAME VARCHAR2(256 CHAR) NULL ,
    BIZ_DATE DATE NULL ,
    ENTRY_NUM NUMBER(20,5) NULL,
    ENTRY_VALUE NUMBER(20,5) NULL,
    RETURN_RATE NUMBER(20,5) NULL,
    INSPECTION_RATE NUMBER(20,5) NULL,
    NOPASS_NUM NUMBER(20,5) NULL,
    NEW_HSCODE_LIST VARCHAR2(2000 CHAR)  NULL ,
    RISK_LABEL VARCHAR2(1000 CHAR) NULL ,
    SCORE NUMBER(20,5) NULL,
    CHECK_TIME DATE NOT NULL ,
    ISCURRENT NUMBER(10) NOT NULL ,
    LASTUPDATE DATE NOT NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.ORG_CODE IS '经营企业信用代码';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.ORG_NAME  IS '经营企业名称';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.BIZ_DATE IS '业务时间';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.ENTRY_NUM IS '报关单量';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.ENTRY_VALUE IS '报关单货值';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.RETURN_RATE IS '退单率';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.INSPECTION_RATE IS '查验率';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.NOPASS_NUM IS '查获量';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.NEW_HSCODE_LIST IS '陌生货类';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.RISK_LABEL IS '异常标签';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.SCORE IS '模型扣分';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.CHECK_TIME IS '模型运行时间';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.ISCURRENT IS '是否使用：1(使用)；0(不使用)';
COMMENT ON COLUMN BD_RISK_RESULT_TRADE_TD2.LASTUPDATE IS '数据最后更新时间';

ALTER TABLE BD_RISK_RESULT_TRADE_TD2 ADD CHECK (ID IS NOT NULL);

ALTER TABLE BD_RISK_RESULT_TRADE_TD2 ADD PRIMARY KEY (ID);