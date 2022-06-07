create table T_JUDGEMENT (
ID NUMBER(10) not null,
COMPANY_ID VARCHAR2(64 char),
COMPANY_NAME VARCHAR2(64 char),
CASE_ID VARCHAR2(64 char) NULL,
CASE_NO VARCHAR2(255 char) NULL,
CASE_REASON_TYPE VARCHAR2(64 char) NULL,
SUBMIT_DATE date NULL,
ISCURRENT NUMBER(1) NULL,
LASTUPDATE DATE NULL
)
LOGGING
NOCOMPRESS
NOCACHE
;

-- ----------------------------
-- Indexes structure for table BD_RISK_CROSS_TRADE_SCORE
-- ----------------------------

-- ----------------------------
-- Checks structure for table BD_RISK_CROSS_TRADE_SCORE
-- ----------------------------
ALTER TABLE T_JUDGEMENT ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE T_JUDGEMENT ADD PRIMARY KEY (ID);