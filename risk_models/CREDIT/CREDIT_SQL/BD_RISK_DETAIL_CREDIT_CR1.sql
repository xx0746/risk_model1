CREATE TABLE BD_RISK_DETAIL_CREDIT_CR1 (
ID NUMBER(20) NOT NULL ,
ORG_CODE VARCHAR2(64 CHAR) NOT NULL ,
ORG_NAME VARCHAR2(64 CHAR) NOT NULL ,
CASE_ID VARCHAR2(64 CHAR) NULL ,
CASE_INFO VARCHAR2(64 CHAR) NULL ,
CASE_TIME DATE NULL,
STATUS VARCHAR2(64 CHAR) NULL ,
ROLE VARCHAR2(64 CHAR) NULL ,
EVENT_TYPE VARCHAR2(64 CHAR) NULL ,
CHECK_TIME DATE NOT NULL,
ISCURRENT NUMBER(10) NOT NULL,
LASTUPDATE DATE NOT NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

COMMENT ON COLUMN BD_RISK_DETAIL_CREDIT_CR1.ID IS 'ID';
COMMENT ON COLUMN BD_RISK_DETAIL_CREDIT_CR1.ORG_CODE IS '公司社会信用代码';
COMMENT ON COLUMN BD_RISK_DETAIL_CREDIT_CR1.ORG_NAME IS '公司名称';
COMMENT ON COLUMN BD_RISK_DETAIL_CREDIT_CR1.CASE_ID IS '案件号';
COMMENT ON COLUMN BD_RISK_DETAIL_CREDIT_CR1.CASE_INFO IS '案件信息';
COMMENT ON COLUMN BD_RISK_DETAIL_CREDIT_CR1.CASE_TIME IS '案件时间';
COMMENT ON COLUMN BD_RISK_DETAIL_CREDIT_CR1.STATUS IS '案件状态';
COMMENT ON COLUMN BD_RISK_DETAIL_CREDIT_CR1.ROLE IS '案件中身份';
COMMENT ON COLUMN BD_RISK_DETAIL_CREDIT_CR1.EVENT_TYPE IS '案件类型';
COMMENT ON COLUMN BD_RISK_DETAIL_CREDIT_CR1.CHECK_TIME IS '运行时间';
COMMENT ON COLUMN BD_RISK_DETAIL_CREDIT_CR1.ISCURRENT IS '是否使用';
COMMENT ON COLUMN BD_RISK_DETAIL_CREDIT_CR1.LASTUPDATE IS '数据最后更新时间';


-- ----------------------------
-- Indexes structure for table BD_RISK_DETAIL_CREDIT_CR1
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_DETAIL_CREDIT_CR1
-- ----------------------------
ALTER TABLE BD_RISK_DETAIL_CREDIT_CR1 ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE BD_RISK_DETAIL_CREDIT_CR1 ADD PRIMARY KEY (ID);