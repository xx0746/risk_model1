CREATE TABLE BD_RISK_RESULT_CREDIT_CR2 (
ID NUMBER(20) NOT NULL ,
ORG_CODE VARCHAR2(64 CHAR) NOT NULL ,
ORG_NAME VARCHAR2(64 CHAR) NOT NULL ,
INVESTOR VARCHAR2(64 CHAR) NULL ,
CASE_ID VARCHAR2(64 CHAR) NULL ,
CASE_INFO VARCHAR2(64 CHAR) NULL ,
CASE_TIME DATE NULL,
STATUS VARCHAR2(64 CHAR) NULL ,
ROLE VARCHAR2(64 CHAR) NULL ,
EVENT_TYPE VARCHAR2(64 CHAR) NULL ,
RISK_LABEL VARCHAR2(64 CHAR) NOT NULL,
SCORE NUMBER(20,5) NOT NULL,
CHECK_TIME DATE NOT NULL,
ISCURRENT NUMBER(10) NOT NULL,
LASTUPDATE DATE NOT NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.ORG_CODE IS '公司社会信用代码';
COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.ORG_NAME IS '公司名称';
COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.INVESTOR IS '投资方名称';
COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.CASE_ID IS '案件号';
COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.CASE_INFO IS '案件信息';
COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.CASE_TIME IS '案件时间';
COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.STATUS IS '案件状态';
COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.ROLE IS '投资方案件中身份';
COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.EVENT_TYPE IS '案件类型';
COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.RISK_LABEL IS '风控标签';
COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.SCORE IS '分数';
COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.CHECK_TIME IS '运行时间';
COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.ISCURRENT IS '是否使用';
COMMENT ON COLUMN BD_RISK_RESULT_CREDIT_CR2.LASTUPDATE IS '数据最后更新时间';


-- ----------------------------
-- Indexes structure for table BD_RISK_RESULT_CREDIT_CR2
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_RESULT_CREDIT_CR2
-- ----------------------------
ALTER TABLE BD_RISK_RESULT_CREDIT_CR2 ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE BD_RISK_RESULT_CREDIT_CR2 ADD PRIMARY KEY (ID);