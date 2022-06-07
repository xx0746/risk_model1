CREATE TABLE BD_RISK_RESULT_FINANCE_FN1 (
ID NUMBER(20) NOT NULL ,
ORG_CODE VARCHAR2(64 CHAR) NOT NULL ,
OBJ_CODE VARCHAR2(128 CHAR) NOT NULL ,
BATCH_TYPE VARCHAR2(128 CHAR) NOT NULL ,
TRADE_TOTAL NUMBER(20,5) NULL,
RISK_LABEL VARCHAR2(64 CHAR) NOT NULL ,
SCORE NUMBER(20,5) NULL,
CHECK_TIME DATE NOT NULL,
ISCURRENT NUMBER(10) NOT NULL,
LASTUPDATE DATE NOT NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_RESULT_FINANCE_FN1.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_RESULT_FINANCE_FN1.ORG_CODE IS '公司社会信用代码';
COMMENT ON COLUMN BD_RISK_RESULT_FINANCE_FN1.OBJ_CODE IS '成品批次号';
COMMENT ON COLUMN BD_RISK_RESULT_FINANCE_FN1.BATCH_TYPE IS '成品类型';
COMMENT ON COLUMN BD_RISK_RESULT_FINANCE_FN1.TRADE_TOTAL IS '批次总成本';
COMMENT ON COLUMN BD_RISK_RESULT_FINANCE_FN1.RISK_LABEL IS '风控标签';
COMMENT ON COLUMN BD_RISK_RESULT_FINANCE_FN1.SCORE IS '风控分数';
COMMENT ON COLUMN BD_RISK_RESULT_FINANCE_FN1.CHECK_TIME IS '运行时间';
COMMENT ON COLUMN BD_RISK_RESULT_FINANCE_FN1.ISCURRENT IS '是否使用';
COMMENT ON COLUMN BD_RISK_RESULT_FINANCE_FN1.LASTUPDATE IS '数据最后更新时间';


-- ----------------------------
-- Indexes structure for table BD_RISK_RESULT_FINANCE_FN1
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_RESULT_FINANCE_FN1
-- ----------------------------
ALTER TABLE BD_RISK_RESULT_FINANCE_FN1 ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE BD_RISK_RESULT_FINANCE_FN1 ADD PRIMARY KEY (ID);