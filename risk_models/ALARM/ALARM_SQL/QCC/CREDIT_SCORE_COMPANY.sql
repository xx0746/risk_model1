create table CREDIT_SCORE_COMPANY (
ID NUMBER(10) not null,
CREDIT_CODE VARCHAR2(64 char) NULL,
COMPANY_NAME VARCHAR2(64 char) NULL,
COMPANY_CODE VARCHAR2(64 char) NULL,
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
ALTER TABLE CREDIT_SCORE_COMPANY ADD CHECK (ID IS NOT NULL);

-- ----------------------------
-- Primary Key structure for table UPLOAD_FILE
-- ----------------------------
ALTER TABLE CREDIT_SCORE_COMPANY ADD PRIMARY KEY (ID);